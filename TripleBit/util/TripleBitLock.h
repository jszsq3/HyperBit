/*
 * TripleBitLock.h
 *
 *  Created on: 2017年12月6日
 *      Author: XuQingQing
 */

#ifndef TRIPLEBIT_UTIL_TRIPLEBITLOCK_H_
#define TRIPLEBIT_UTIL_TRIPLEBITLOCK_H_

#include <atomic>


//base on CAS
class TripleBitMutexLock
{
private:
    static constexpr int UNLOCKED = 0;
    static constexpr int LOCKED = 1;

    std::atomic<int> m_value;

public:

    void lock()
    {
        while (true)
        {
            int expected = UNLOCKED;
            if (m_value.compare_exchange_strong(expected, LOCKED))
                break;
        }
    }

    void unlock()
    {
        m_value.store(UNLOCKED);
    }
};



#endif /* TRIPLEBIT_UTIL_TRIPLEBITLOCK_H_ */
