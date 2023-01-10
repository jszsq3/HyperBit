//
// Created by 臧少琦 on 2022/3/13.
//

#include "LogmapBuffer.h"

#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "MMapBuffer.h"
#include "TempFile.h"
#include "TempMMapBuffer.h"

/*LogChunkManager*/
LogChunkManager::LogChunkManager(){

}
LogChunkManager::LogChunkManager(LogmapBuffer *logmap):logmap(logmap){
    if(logmap->usedPage>=logmap->templogbuffer->get_page_count()){
        logmap->mutex_logmap.lock();
        logmap->mmapaddres = logmap->templogbuffer->resize(MemoryBuffer::pagesize);
        first_page_no = logmap->templogbuffer->get_page_count();
        final_page_no = first_page_no;
        logmap->usedPage++;
        logmap->mutex_logmap.unlock();
    }else{
        logmap->usedPage++;
        first_page_no = logmap->usedPage;
        final_page_no = first_page_no;
    }
}

void LogChunkManager::init_logChunkManager(LogmapBuffer *logmap, LogChunkMeta* meta, ID pid, int first_page_no, int final_page_no){
    this->logmap = logmap;
    this->meta = meta;
    this->pid = pid;
    this->first_page_no = first_page_no;
    this->final_page_no = final_page_no;
}

LogChunkMeta* LogChunkManager::get_Meta(){
    return logmap->get_Page(first_page_no);
}

void LogChunkManager::readOPIDIDTS(uchar *&reader, UpdateOp &op, ID &subjectID, ID &objectID, TimeStamp &ts){
    op = *(UpdateOp*)reader;
    reader += sizeof(UpdateOp);
    subjectID = *(ID*)reader;
    reader += sizeof(ID);
    objectID = *(ID*)reader;
    reader += sizeof(ID);
    ts = *(TimeStamp*)reader;
    reader += sizeof(TimeStamp);

}

void LogChunkManager::insertLog(ID subjectID, ID objectID, TimeStamp ts){

    size_t len = sizeof(UpdateOp)+sizeof(ID)*2+sizeof(TimeStamp);
    MMapBuffer *tempog_Mmap = logmap->templogbuffer;
    LogChunkMeta* first_meta = logmap->get_Page(first_page_no);
    LogChunkMeta* final_meta = logmap->get_Page(final_page_no);
    uchar *writer;
    if(first_meta->used_space==0){
        first_meta->page_no=first_page_no;
        first_meta->next_page_no = -1;
        first_meta->used_space = sizeof(LogChunkMeta);
        first_meta->min_ts = ts;
        first_meta->max_ts = ts;
        first_meta->pid = pid;

    }else if(final_meta->used_space+len > MemoryBuffer::pagesize){
        int new_page_no;
        if(logmap->usedPage>=logmap->templogbuffer->get_page_count()){
            logmap->mutex_logmap.lock();
            logmap->mmapaddres = tempog_Mmap->resize(MemoryBuffer::pagesize);
            new_page_no = tempog_Mmap->get_page_count();
            logmap->usedPage++;
            logmap->mutex_logmap.unlock();
        }else{
            logmap->usedPage++;
            new_page_no = logmap->usedPage;
        }
        

        final_meta = logmap->get_Page(final_page_no);
        final_meta->next_page_no = new_page_no;
        final_page_no = new_page_no;
        
        final_meta = logmap->get_Page(final_page_no);
        final_meta->page_no=new_page_no;
        final_meta->next_page_no = -1;
        final_meta->used_space = sizeof(LogChunkMeta);
        final_meta->min_ts = ts;
        final_meta->max_ts = ts;
        final_meta->pid = pid;

    }else{
    }
    final_meta->max_ts = ts>final_meta->max_ts?ts:final_meta->max_ts;
    writer = (uchar*)final_meta+final_meta->used_space;
    *(UpdateOp*)writer = INSERT;
    writer += sizeof(UpdateOp);
    *(ID*)writer = subjectID;
    writer += sizeof(ID);
    *(ID*)writer = objectID;
    writer += sizeof(ID);
    *(TimeStamp*)writer = ts;
    writer += sizeof(TimeStamp);
    final_meta->used_space += len;
    // cout<<final_meta->pid<<endl;

}

void LogChunkManager::findObjectBySubjectAndPredicate(ID subjectID, TimeStamp basis_ts, TimeStamp target_ts, set<vector<ID>> &results, vector<ID> &max_min, vector<ID> &tmp_max_min){
#ifdef ZSQDEBUG
    ofstream out("./mmm");
#endif
    LogChunkMeta* logmeta = meta;
    uchar *reader = (uchar*)meta;
    while(logmeta->next_page_no!=-1){
        if(basis_ts<target_ts){//正向
            if(basis_ts<=logmeta->max_ts&&logmeta->min_ts<=target_ts){
#ifdef ZSQDEBUG
                cout<<"meta->min_ts : "<<logmeta->min_ts<<endl;
                cout<<"meta->max_ts : "<<logmeta->max_ts<<endl;
                cout<<"meta->next_page_no : "<<logmeta->next_page_no<<endl;
                cout<<"meta->page_no : "<<logmeta->page_no<<endl;
                cout<<"meta->pid : "<<logmeta->pid<<endl;
                cout<<"meta->used_space : "<<logmeta->used_space<<endl;
#endif
                UpdateOp op;
                ID sid,oid;
                TimeStamp ts;
                uchar* limit = reader+logmeta->used_space;
                reader = reader+sizeof(LogChunkMeta);
                while(reader<limit){
                    op = *(UpdateOp*)reader;
                    reader += sizeof(UpdateOp);
                    sid = *(ID*)reader;
                    reader += sizeof(ID);
                    oid = *(ID*)reader;
                    reader += sizeof(ID);
                    ts = *(TimeStamp*)reader;
                    reader += sizeof(TimeStamp);
#ifdef ZSQDEBUG
                    out<<op<<" "<<sid<<" "<<oid<<" "<<ts<<endl;
#endif
                    if(ts>=basis_ts&&ts<=target_ts){
                        if(subjectID==sid){
                            if(op==UpdateOp::INSERT){
                                if(oid>=max_min[0]&&oid<=max_min[1]){
                                    results.insert({oid});
                                    tmp_max_min[0] = oid<tmp_max_min[0]?oid:tmp_max_min[0];
                                    tmp_max_min[1] = oid>tmp_max_min[1]?oid:tmp_max_min[1];
                                }
                            }else if(op==UpdateOp::DELETE){
                                results.erase({oid});
                            }
                        }
                    }
                }
#ifdef ZSQDEBUG
                cout<<subjectID<<" "<<basis_ts<<" "<<target_ts<<endl;
#endif
            }else if(logmeta->min_ts>target_ts){
                break;
            }
        }else if(basis_ts>target_ts){//反向
            if(target_ts<=logmeta->max_ts&&logmeta->min_ts<=basis_ts){
#ifdef ZSQDEBUG
                cout<<"meta->min_ts : "<<logmeta->min_ts<<endl;
                cout<<"meta->max_ts : "<<logmeta->max_ts<<endl;
                cout<<"meta->next_page_no : "<<logmeta->next_page_no<<endl;
                cout<<"meta->page_no : "<<logmeta->page_no<<endl;
                cout<<"meta->pid : "<<logmeta->pid<<endl;
                cout<<"meta->used_space : "<<logmeta->used_space<<endl;
#endif
                UpdateOp op;
                ID sid,oid;
                TimeStamp ts;
                uchar* limit = reader+logmeta->used_space;
                reader = reader+sizeof(LogChunkMeta);
                while(reader<limit){
                    op = *(UpdateOp*)reader;
                    reader += sizeof(UpdateOp);
                    sid = *(ID*)reader;
                    reader += sizeof(ID);
                    oid = *(ID*)reader;
                    reader += sizeof(ID);
                    ts = *(TimeStamp*)reader;
                    reader += sizeof(TimeStamp);
#ifdef ZSQDEBUG
                    out<<op<<" "<<sid<<" "<<oid<<" "<<ts<<endl;
#endif
                    if(ts>=basis_ts&&ts<=target_ts){
                        if(subjectID==sid){
                            if(op==UpdateOp::INSERT){
                                results.erase({oid});
                            }else if(op==UpdateOp::DELETE){
                                if(oid>=max_min[0]&&oid<=max_min[1]){
                                    results.insert({oid});
                                    tmp_max_min[0] = oid<tmp_max_min[0]?oid:tmp_max_min[0];
                                    tmp_max_min[1] = oid>tmp_max_min[1]?oid:tmp_max_min[1];
                                }
                            }
                        }
                    }
                }
#ifdef ZSQDEBUG
                cout<<subjectID<<" "<<basis_ts<<" "<<target_ts<<endl;
#endif
            }else if(logmeta->min_ts>basis_ts){
                break;
            }
        }
        logmeta = (LogChunkMeta*)logmap->get_Page(logmeta->next_page_no);
        reader = (uchar*)logmeta;
    }

    //循环外最后一次
    
    if(basis_ts<target_ts){//正向
        if(basis_ts<=logmeta->max_ts&&logmeta->min_ts<=target_ts){

            UpdateOp op;
            ID sid,oid;
            TimeStamp ts;
            uchar* limit = reader+logmeta->used_space;
            reader = reader+sizeof(LogChunkMeta);
            while(reader<limit){
                op = *(UpdateOp*)reader;
                reader += sizeof(UpdateOp);
                sid = *(ID*)reader;
                reader += sizeof(ID);
                oid = *(ID*)reader;
                reader += sizeof(ID);
                ts = *(TimeStamp*)reader;
                reader += sizeof(TimeStamp);

                if(ts>=basis_ts&&ts<=target_ts){
                    if(subjectID==sid){
                        if(op==UpdateOp::INSERT){
                            if(oid>=max_min[0]&&oid<=max_min[1]){
                                results.insert({oid});
                                tmp_max_min[0] = oid<tmp_max_min[0]?oid:tmp_max_min[0];
                                tmp_max_min[1] = oid>tmp_max_min[1]?oid:tmp_max_min[1];
                            }
                        }else if(op==UpdateOp::DELETE){
                            results.erase({oid});
                        }
                    }
                }
            }

        }else if(logmeta->min_ts>target_ts){
            // break;
        }
    }else if(basis_ts>target_ts){//反向
        if(target_ts<=logmeta->max_ts&&logmeta->min_ts<=basis_ts){

            UpdateOp op;
            ID sid,oid;
            TimeStamp ts;
            uchar* limit = reader+logmeta->used_space;
            reader = reader+sizeof(LogChunkMeta);
            while(reader<limit){
                op = *(UpdateOp*)reader;
                reader += sizeof(UpdateOp);
                sid = *(ID*)reader;
                reader += sizeof(ID);
                oid = *(ID*)reader;
                reader += sizeof(ID);
                ts = *(TimeStamp*)reader;
                reader += sizeof(TimeStamp);

                if(ts>=basis_ts&&ts<=target_ts){
                    if(subjectID==sid){
                        if(op==UpdateOp::INSERT){
                            results.erase({oid});
                        }else if(op==UpdateOp::DELETE){
                            if(oid>=max_min[0]&&oid<=max_min[1]){
                                results.insert({oid});
                                tmp_max_min[0] = oid<tmp_max_min[0]?oid:tmp_max_min[0];
                                tmp_max_min[1] = oid>tmp_max_min[1]?oid:tmp_max_min[1];
                            }
                        }
                    }
                }
            }

        }else if(logmeta->min_ts>basis_ts){
            // break;
        }
    }
}

void LogChunkManager::findSubjectByObjectAndPredicate(ID objectID, TimeStamp basis_ts, TimeStamp target_ts, set<vector<ID>> &results, vector<ID> &max_min, vector<ID> &tmp_max_min){
#ifdef ZSQDEBUG
    ofstream out("./mmm");
#endif
    LogChunkMeta* logmeta = meta;
    uchar *reader = (uchar*)meta;
    while(logmeta->next_page_no!=-1){
        if(basis_ts<target_ts){//正向
            if(basis_ts<=logmeta->max_ts&&logmeta->min_ts<=target_ts){
#ifdef ZSQDEBUG
                cout<<"meta->min_ts : "<<logmeta->min_ts<<endl;
                cout<<"meta->max_ts : "<<logmeta->max_ts<<endl;
                cout<<"meta->next_page_no : "<<logmeta->next_page_no<<endl;
                cout<<"meta->page_no : "<<logmeta->page_no<<endl;
                cout<<"meta->pid : "<<logmeta->pid<<endl;
                cout<<"meta->used_space : "<<logmeta->used_space<<endl;
#endif
                UpdateOp op;
                ID sid,oid;
                TimeStamp ts;
                uchar* limit = reader+logmeta->used_space;
                reader = reader+sizeof(LogChunkMeta);
                while(reader<limit){
                    op = *(UpdateOp*)reader;
                    reader += sizeof(UpdateOp);
                    sid = *(ID*)reader;
                    reader += sizeof(ID);
                    oid = *(ID*)reader;
                    reader += sizeof(ID);
                    ts = *(TimeStamp*)reader;
                    reader += sizeof(TimeStamp);
#ifdef ZSQDEBUG
                    out<<op<<" "<<sid<<" "<<oid<<" "<<ts<<endl;
#endif
                    if(ts>=basis_ts&&ts<=target_ts){
                        if(objectID==oid){
                            if(op==UpdateOp::INSERT){
                                if(sid>=max_min[0]&&sid<=max_min[1]){
                                    results.insert({sid});
                                    tmp_max_min[0] = sid<tmp_max_min[0]?sid:tmp_max_min[0];
                                    tmp_max_min[1] = sid>tmp_max_min[1]?sid:tmp_max_min[1];
                                }
                            }else if(op==UpdateOp::DELETE){
                                results.erase({sid});
                            }
                        }
                    }
                }
#ifdef ZSQDEBUG
                cout<<subjectID<<" "<<basis_ts<<" "<<target_ts<<endl;
#endif
            }else if(logmeta->min_ts>target_ts){
                break;
            }
        }else if(basis_ts>target_ts){//反向
            if(target_ts<=logmeta->max_ts&&logmeta->min_ts<=basis_ts){
#ifdef ZSQDEBUG
                cout<<"meta->min_ts : "<<logmeta->min_ts<<endl;
                cout<<"meta->max_ts : "<<logmeta->max_ts<<endl;
                cout<<"meta->next_page_no : "<<logmeta->next_page_no<<endl;
                cout<<"meta->page_no : "<<logmeta->page_no<<endl;
                cout<<"meta->pid : "<<logmeta->pid<<endl;
                cout<<"meta->used_space : "<<logmeta->used_space<<endl;
#endif
                UpdateOp op;
                ID sid,oid;
                TimeStamp ts;
                uchar* limit = reader+logmeta->used_space;
                reader = reader+sizeof(LogChunkMeta);
                while(reader<limit){
                    op = *(UpdateOp*)reader;
                    reader += sizeof(UpdateOp);
                    sid = *(ID*)reader;
                    reader += sizeof(ID);
                    oid = *(ID*)reader;
                    reader += sizeof(ID);
                    ts = *(TimeStamp*)reader;
                    reader += sizeof(TimeStamp);
#ifdef ZSQDEBUG
                    out<<op<<" "<<sid<<" "<<oid<<" "<<ts<<endl;
#endif
                    if(ts>=basis_ts&&ts<=target_ts){
                        if(objectID==oid){
                            if(op==UpdateOp::INSERT){
                                results.erase({sid});
                            }else if(op==UpdateOp::DELETE){
                                if(sid>=max_min[0]&&sid<=max_min[1]){
                                    results.insert({sid});
                                    tmp_max_min[0] = sid<tmp_max_min[0]?sid:tmp_max_min[0];
                                    tmp_max_min[1] = sid>tmp_max_min[1]?sid:tmp_max_min[1];
                                }
                            }
                        }
                    }
                }
#ifdef ZSQDEBUG
                cout<<subjectID<<" "<<basis_ts<<" "<<target_ts<<endl;
#endif
            }else if(logmeta->min_ts>basis_ts){
                break;
            }
        }
        logmeta = (LogChunkMeta*)logmap->get_Page(logmeta->next_page_no);
        reader = (uchar*)logmeta;
    }

    //循环外最后一次
    if(basis_ts<target_ts){//正向
        if(basis_ts<=logmeta->max_ts&&logmeta->min_ts<=target_ts){

            UpdateOp op;
            ID sid,oid;
            TimeStamp ts;
            uchar* limit = reader+logmeta->used_space;
            reader = reader+sizeof(LogChunkMeta);
            while(reader<limit){
                op = *(UpdateOp*)reader;
                reader += sizeof(UpdateOp);
                sid = *(ID*)reader;
                reader += sizeof(ID);
                oid = *(ID*)reader;
                reader += sizeof(ID);
                ts = *(TimeStamp*)reader;
                reader += sizeof(TimeStamp);

                if(ts>=basis_ts&&ts<=target_ts){
                    if(objectID==oid){
                        if(op==UpdateOp::INSERT){
                            if(sid>=max_min[0]&&sid<=max_min[1]){
                                results.insert({sid});
                                tmp_max_min[0] = sid<tmp_max_min[0]?sid:tmp_max_min[0];
                                tmp_max_min[1] = sid>tmp_max_min[1]?sid:tmp_max_min[1];
                            }
                        }else if(op==UpdateOp::DELETE){
                            results.erase({sid});
                        }
                    }
                }
            }

        }else if(logmeta->min_ts>target_ts){
            // break;
        }
    }else if(basis_ts>target_ts){//反向
        if(target_ts<=logmeta->max_ts&&logmeta->min_ts<=basis_ts){

            UpdateOp op;
            ID sid,oid;
            TimeStamp ts;
            uchar* limit = reader+logmeta->used_space;
            reader = reader+sizeof(LogChunkMeta);
            while(reader<limit){
                op = *(UpdateOp*)reader;
                reader += sizeof(UpdateOp);
                sid = *(ID*)reader;
                reader += sizeof(ID);
                oid = *(ID*)reader;
                reader += sizeof(ID);
                ts = *(TimeStamp*)reader;
                reader += sizeof(TimeStamp);

                if(ts>=basis_ts&&ts<=target_ts){
                    if(objectID==oid){
                        if(op==UpdateOp::INSERT){
                            results.erase({sid});
                        }else if(op==UpdateOp::DELETE){
                            if(sid>=max_min[0]&&sid<=max_min[1]){
                                results.insert({sid});
                                tmp_max_min[0] = sid<tmp_max_min[0]?sid:tmp_max_min[0];
                                tmp_max_min[1] = sid>tmp_max_min[1]?sid:tmp_max_min[1];
                            }
                        }
                    }
                }
            }

        }else if(logmeta->min_ts>basis_ts){
            // break;
        }
    }
}


void LogChunkManager::findSubjectAndObjectByPredicate(TimeStamp basis_ts, TimeStamp target_ts, set<vector<ID>> &results, vector<ID> &max_min1, vector<ID> &max_min2, vector<ID> &tmp_max_min1, vector<ID> &tmp_max_min2){
#ifdef ZSQDEBUG
    ofstream out("./mmm");
#endif
    LogChunkMeta* logmeta = meta;
    uchar *reader = (uchar*)meta;
    while(logmeta->next_page_no!=-1){
        if(basis_ts<target_ts){//正向
            if(basis_ts<=logmeta->max_ts&&logmeta->min_ts<=target_ts){
#ifdef ZSQDEBUG
                cout<<"meta->min_ts : "<<logmeta->min_ts<<endl;
                cout<<"meta->max_ts : "<<logmeta->max_ts<<endl;
                cout<<"meta->next_page_no : "<<logmeta->next_page_no<<endl;
                cout<<"meta->page_no : "<<logmeta->page_no<<endl;
                cout<<"meta->pid : "<<logmeta->pid<<endl;
                cout<<"meta->used_space : "<<logmeta->used_space<<endl;
#endif
                UpdateOp op;
                ID sid,oid;
                TimeStamp ts;
                uchar* limit = reader+logmeta->used_space;
                reader = reader+sizeof(LogChunkMeta);
                while(reader<limit){
                    op = *(UpdateOp*)reader;
                    reader += sizeof(UpdateOp);
                    sid = *(ID*)reader;
                    reader += sizeof(ID);
                    oid = *(ID*)reader;
                    reader += sizeof(ID);
                    ts = *(TimeStamp*)reader;
                    reader += sizeof(TimeStamp);
#ifdef ZSQDEBUG
                    out<<op<<" "<<sid<<" "<<oid<<" "<<ts<<endl;
#endif
                    if(ts>=basis_ts&&ts<=target_ts){
                        if(op==UpdateOp::INSERT){
                            results.insert({sid,oid});
                            if(sid>=max_min1[0]&&sid<=max_min1[1]&&oid>=max_min2[0]&&oid<=max_min2[1]){
                                results.insert({sid,oid});
                                tmp_max_min1[0] = sid<tmp_max_min1[0]?sid:tmp_max_min1[0];
                                tmp_max_min1[1] = sid>tmp_max_min1[1]?sid:tmp_max_min1[1];
                                tmp_max_min2[0] = oid<tmp_max_min2[0]?oid:tmp_max_min2[0];
                                tmp_max_min2[1] = oid>tmp_max_min2[1]?oid:tmp_max_min2[1];
                            }
                        }else if(op==UpdateOp::DELETE){
                            results.erase({sid,oid});
                        }
                    }
                }
#ifdef ZSQDEBUG
                cout<<subjectID<<" "<<basis_ts<<" "<<target_ts<<endl;
#endif
            }else if(logmeta->min_ts>target_ts){
                break;
            }
        }else if(basis_ts>target_ts){//反向
            if(target_ts<=logmeta->max_ts&&logmeta->min_ts<=basis_ts){
#ifdef ZSQDEBUG
                cout<<"meta->min_ts : "<<logmeta->min_ts<<endl;
                cout<<"meta->max_ts : "<<logmeta->max_ts<<endl;
                cout<<"meta->next_page_no : "<<logmeta->next_page_no<<endl;
                cout<<"meta->page_no : "<<logmeta->page_no<<endl;
                cout<<"meta->pid : "<<logmeta->pid<<endl;
                cout<<"meta->used_space : "<<logmeta->used_space<<endl;
#endif
                UpdateOp op;
                ID sid,oid;
                TimeStamp ts;
                uchar* limit = reader+logmeta->used_space;
                reader = reader+sizeof(LogChunkMeta);
                while(reader<limit){
                    op = *(UpdateOp*)reader;
                    reader += sizeof(UpdateOp);
                    sid = *(ID*)reader;
                    reader += sizeof(ID);
                    oid = *(ID*)reader;
                    reader += sizeof(ID);
                    ts = *(TimeStamp*)reader;
                    reader += sizeof(TimeStamp);
#ifdef ZSQDEBUG
                    out<<op<<" "<<sid<<" "<<oid<<" "<<ts<<endl;
#endif
                    if(ts>=basis_ts&&ts<=target_ts){
                        if(op==UpdateOp::INSERT){
                            results.erase({sid,oid});
                        }else if(op==UpdateOp::DELETE){
                            if(sid>=max_min1[0]&&sid<=max_min1[1]&&oid>=max_min2[0]&&oid<=max_min2[1]){
                                results.insert({sid,oid});
                                tmp_max_min1[0] = sid<tmp_max_min1[0]?sid:tmp_max_min1[0];
                                tmp_max_min1[1] = sid>tmp_max_min1[1]?sid:tmp_max_min1[1];
                                tmp_max_min2[0] = oid<tmp_max_min2[0]?oid:tmp_max_min2[0];
                                tmp_max_min2[1] = oid>tmp_max_min2[1]?oid:tmp_max_min2[1];
                            }
                        }
                    }
                }
#ifdef ZSQDEBUG
                cout<<subjectID<<" "<<basis_ts<<" "<<target_ts<<endl;
#endif
            }else if(logmeta->min_ts>basis_ts){
                break;
            }
        }
        logmeta = (LogChunkMeta*)logmap->get_Page(logmeta->next_page_no);
        reader = (uchar*)logmeta;
    }
    //循环外最后一次扫描
    if(basis_ts<target_ts){//正向
        if(basis_ts<=logmeta->max_ts&&logmeta->min_ts<=target_ts){

            UpdateOp op;
            ID sid,oid;
            TimeStamp ts;
            uchar* limit = reader+logmeta->used_space;
            reader = reader+sizeof(LogChunkMeta);
            while(reader<limit){
                op = *(UpdateOp*)reader;
                reader += sizeof(UpdateOp);
                sid = *(ID*)reader;
                reader += sizeof(ID);
                oid = *(ID*)reader;
                reader += sizeof(ID);
                ts = *(TimeStamp*)reader;
                reader += sizeof(TimeStamp);

                if(ts>=basis_ts&&ts<=target_ts){
                    if(op==UpdateOp::INSERT){
                        if(sid>=max_min1[0]&&sid<=max_min1[1]&&oid>=max_min2[0]&&oid<=max_min2[1]){
                            results.insert({sid,oid});
                            tmp_max_min1[0] = sid<tmp_max_min1[0]?sid:tmp_max_min1[0];
                            tmp_max_min1[1] = sid>tmp_max_min1[1]?sid:tmp_max_min1[1];
                            tmp_max_min2[0] = oid<tmp_max_min2[0]?oid:tmp_max_min2[0];
                            tmp_max_min2[1] = oid>tmp_max_min2[1]?oid:tmp_max_min2[1];
                        }
                    }else if(op==UpdateOp::DELETE){
                        results.erase({sid,oid});
                    }
                }
            }

        }else if(logmeta->min_ts>target_ts){
            // break;
        }
    }else if(basis_ts>target_ts){//反向
        if(target_ts<=logmeta->max_ts&&logmeta->min_ts<=basis_ts){

            UpdateOp op;
            ID sid,oid;
            TimeStamp ts;
            uchar* limit = reader+logmeta->used_space;
            reader = reader+sizeof(LogChunkMeta);
            while(reader<limit){
                op = *(UpdateOp*)reader;
                reader += sizeof(UpdateOp);
                sid = *(ID*)reader;
                reader += sizeof(ID);
                oid = *(ID*)reader;
                reader += sizeof(ID);
                ts = *(TimeStamp*)reader;
                reader += sizeof(TimeStamp);

                if(ts>=basis_ts&&ts<=target_ts){
                    if(op==UpdateOp::INSERT){
                        results.erase({sid,oid});
                    }else if(op==UpdateOp::DELETE){
                        if(sid>=max_min1[0]&&sid<=max_min1[1]&&oid>=max_min2[0]&&oid<=max_min2[1]){
                            results.insert({sid,oid});
                            tmp_max_min1[0] = sid<tmp_max_min1[0]?sid:tmp_max_min1[0];
                            tmp_max_min1[1] = sid>tmp_max_min1[1]?sid:tmp_max_min1[1];
                            tmp_max_min2[0] = oid<tmp_max_min2[0]?oid:tmp_max_min2[0];
                            tmp_max_min2[1] = oid>tmp_max_min2[1]?oid:tmp_max_min2[1];
                        }
                    }
                }
            }

        }else if(logmeta->min_ts>basis_ts){
            // break;
        }
    }
}
/*LogmapBuffer*/
// LogmapBuffer::LogmapBuffer(){

// }
LogmapBuffer::LogmapBuffer(MMapBuffer *image, const string dir):dir(dir), templogbuffer(image){
    mmapaddres = image->get_address();
}
LogmapBuffer::LogmapBuffer(const string _dir) : dir(_dir)
{
    string filename(dir);
    filename.append("/tmplogbuffer");
    templogbuffer = new MMapBuffer(filename.c_str(), INIT_PAGE_COUNT * MemoryBuffer::pagesize);
    mmapaddres = templogbuffer->get_address();
    usedPage = 0;
    number_of_sp = DEFAULT_SP_COUNT;
}

LogmapBuffer::~LogmapBuffer()
{
}

void LogmapBuffer::flush(){
    templogbuffer->flush();
}


void LogmapBuffer::insertLog(ID subjectID, ID predicateID, ID objectID, TimeStamp ts){
    if(!log_pre_managers.count(predicateID)){
        log_pre_managers[predicateID] = new LogChunkManager(this);
        log_pre_managers[predicateID]->setPID(predicateID);
    }

    log_pre_managers[predicateID]->insertLog(subjectID, objectID, ts);
}

void LogmapBuffer::SnapShotSequence(){
    size_t total_count = 0;
    size_t ave_count = 0;
    vector<TimeStamp> interval;
    // map<TimeStamp, size_t> dp;

    for(auto &i:records_count){
        total_count += i.second;
        dp[i.first] = total_count;
    }
    ave_count = total_count/number_of_sp;
    
    //定区间
    size_t tmp_count = 0;
    map<TimeStamp, size_t>::iterator iter1 = dp.begin();
    map<TimeStamp, size_t>::iterator iter2 = iter1;
    iter2++;
    interval.push_back(iter1->first);
    while(iter2!=dp.end()){
        size_t c1 = iter1->second - dp[interval[interval.size()-1]];
        size_t c2 = iter2->second - dp[interval[interval.size()-1]];
        if(c1<=ave_count&&c2>=ave_count){
            if(ave_count-c1<c2-ave_count){
                interval.push_back(iter1->first);
                    // cout<<iter1->first<<endl;
            }else{
                interval.push_back(iter2->first);
            }
        }
        if(interval.size()==number_of_sp){
            iter1 = dp.end();
            iter1--;
            interval.push_back(iter1->first);
            // cout<<iter1->first<<endl;
            break;
        }
        iter1++;
        iter2++;
    }

    //找区间重心
    
    auto iter3 = interval.begin();
    auto iter4 = iter3; iter4++;
    while(iter4!=interval.end()){
        iter1 = dp.find(*iter3);
        iter2 = dp.find(*iter4);
        // cout<<*iter3<<" "<<*iter4<<endl;
        auto mid = (*iter4-*iter3)/2+*iter3;
        auto miditer = dp.lower_bound(mid); 
        auto miditer1 = miditer;
        if(miditer!=iter1){
            miditer1 = prev(miditer);
        } else{
            sequence_of_sp.push_back(miditer->first);
            iter3++;
            iter4++;
            continue;
        }
        auto midcount = (iter1->second+iter2->second)/2;
        // int k = 0;
        while(true){
            // cout<<iter1->first<<":"<<iter1->second<<" "<<iter2->first<<":"<<iter2->second<<endl;
            // cout<<miditer1->first<<":"<<miditer1->second<<" "<<miditer->first<<":"<<miditer->second<<endl;
            if(miditer->second>=midcount&&miditer1->second<midcount){
                sequence_of_sp.push_back(miditer->first);
                break;
            }else if(miditer->second>midcount&&miditer1->second>=midcount){
                iter2 = prev(miditer);
                mid = (iter2->first-iter1->first)/2 + iter1->first;
                miditer = dp.lower_bound(mid); miditer1 = prev(miditer);
            }else if(miditer->second<midcount&&miditer1->second<midcount){
                iter1 = next(miditer);
                mid = (iter2->first-iter1->first)/2 + iter1->first;
                miditer = dp.lower_bound(mid); miditer1 = prev(miditer);
            }
            
        }
        iter3++;
        iter4++;
    }
    // for(auto&i:sequence_of_sp){
    //     cout<<i<<" "<<dp[i]<<endl;
    // }

    
    
}

void LogmapBuffer::init_for_query(){
    ofstream out("./log_print");
    // const string dir;
    // uchar *mmapaddres;
    // map<ID, LogChunkManager *> log_pre_managers;
    // MMapBuffer *templogbuffer;
    // size_t usedPage;
    // map<TimeStamp, size_t> records_count;
    // int number_of_sp;
    // vector<TimeStamp> sequence_of_sp;
    // mutex mutex_logmap;
    uchar *reader = mmapaddres, *limit = templogbuffer->getSize()+reader;
    while(reader<limit){
        LogChunkMeta *meta = (LogChunkMeta*)reader;
        ID pid = meta->pid;
        // if(pid==0){
        //     reader += MemoryBuffer::pagesize;
        //     continue;
        // }
        out<<"meta->max_ts"<<":"<<meta->max_ts<<endl;
        out<<"meta->min_ts"<<":"<<meta->min_ts<<endl;
        out<<"meta->pid"<<":"<<meta->pid<<endl;
        out<<"meta->page_no"<<":"<<meta->page_no<<endl;
        out<<"meta->next_page_no"<<":"<<meta->next_page_no<<endl;
        out<<"meta->used_space"<<":"<<meta->used_space<<endl;
        out<<"============================"<<endl;
        
        if(log_pre_managers.find(pid)==log_pre_managers.end()){
            log_pre_managers[pid] = new LogChunkManager();
            log_pre_managers[pid]->init_logChunkManager(this, meta, pid, meta->page_no, meta->page_no);
        }else{
            log_pre_managers[pid]->set_final_page_no(meta->page_no);
        }
        // uchar *start_ptr = reader+sizeof(LogChunkMeta), *end_ptr = reader+meta->used_space;
        // while(start_ptr<end_ptr){
        //     UpdateOp op;
        //     ID sid,oid;
        //     TimeStamp ts;
        //     op = *(UpdateOp*)start_ptr;
        //     start_ptr += sizeof(UpdateOp);
        //     sid = *(ID*)start_ptr;
        //     start_ptr += sizeof(ID);
        //     oid = *(ID*)start_ptr;
        //     start_ptr += sizeof(ID);
        //     ts = *(TimeStamp*)start_ptr;
        //     start_ptr += sizeof(TimeStamp);
        //     out<<op<<" "<<sid<<" "<<oid<<" "<<ts<<endl;
        // }
        reader += MemoryBuffer::pagesize;
        usedPage++;
    }

    ifstream records(dir+"/records_num");
    TimeStamp ts;
    size_t record;
    size_t lastrecord = 0;
    while(records>>ts>>record){
        
        dp[ts] = record;
        records_count[ts] = record-lastrecord;
        lastrecord = record;
    }
    records.close();

    ifstream sequence(dir+"/sequence");

    while(sequence>>ts){
        sequence_of_sp.push_back(ts);
    }
    number_of_sp = sequence_of_sp.size();
    records.close();

}

LogmapBuffer* LogmapBuffer::load(MMapBuffer *image, const string dir){

    LogmapBuffer* logmap = new LogmapBuffer(image, dir);
    
    logmap->init_for_query();
    return logmap;
}
void LogmapBuffer::save(){
    ofstream log_records(dir+"/records_num");
    for(auto &i:dp){
        log_records<<i.first<<" "<<i.second<<endl;
    }
    log_records.close();

    ofstream sequence(dir+"/sequence");
    for(auto &i:sequence_of_sp){
        sequence<<i<<" ";
    }
    sequence.close();
}