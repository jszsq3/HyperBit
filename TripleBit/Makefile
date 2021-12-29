SOURCE_DIR := src

# COMPILER := icc
COMPILER := g++
#-pg

# release
BINARY_DIR := bin/lrelease
BINARY_DIR_OBJS := bin/lrelease/TripleBit
CPPFLAGS := -Wall -g -O3  -std=c++11
#LIBS := -L/usr/local/jemalloc/lib -ljemalloc -lpthread -lboost_system -lboost_thread -L/usr/local/lib 
LIBS := -lpthread -lboost_system -lboost_thread -L/usr/local/lib -ltcmalloc -lleveldb

# debug
# BINARY_DIR := bin/ldebug
# CPPFLAGS := -g -fPIC -DDEBUG
COMPILE.cpp = $(COMPILER) $(CFLAGS) $(CPPFLAGS) -c -fPIC
LINK.cpp = $(COMPILER) $(LIBS)

%.o: %.cpp
	#$(call make-depend,$<,$@,$(subst .o,.d,$@))
	$(COMPILE.cpp) $< -o $@


# $(call source-dir-to-binary-dir, directory-list)
source-dir-to-binary-dir = $(addprefix $(BINARY_DIR_OBJS)/,$1)

# $(call source-to-object, source-file-list)
source-to-object = $(call source-dir-to-binary-dir, $(subst .cpp,.o,$1))

# $(subdirectory)
subdirectory = $(patsubst %/module.mk,%,                \
                 $(word                                 \
                   $(words $(MAKEFILE_LIST)),$(MAKEFILE_LIST)))

# $(call make-depend,source-file,object-file,depend-file)
define make-depend
	g++ -MM    \
         -MF$3         \
         -MP            \
         -MT$2         \
         $(CFLAGS)      \
         $(CPPFLAGS)    \
         $(TARGET_ARCH) \
         $1
endef

        
# $(call make-library, library-name, source-file-list)
define make-library
  libraries += $(BINARY_DIR)/$1
  sources   += $2

  $(BINARY_DIR)/$1: $(call source-dir-to-binary-dir, $(subst .cpp,.o,$2))
	g++ -shared -o -fPIC -o $$@ $$^

endef

# $(compile-rules)
define compile-rules
  $(foreach f,$(local_src),$(call one-compile-rule,$(call source-to-object,$f),$f))

endef


# $(call one-compile-rule, binary-file, source-file)
define one-compile-rule
  $1: $2
	$(call make-depend,$2,$1,$(subst .o,.d,$1))
	$(COMPILE.cpp) -o $1 $2

endef

programs     := 
libraries    :=
sources      :=

objects      =  $(call source-to-object,$(sources))
dependencies =  $(subst .o,.d,$(objects))

include_dirs := TripleBit
CPPFLAGS += $(addprefix -I ,$(include_dirs))

MKDIR := mkdir -p
MV    := mv -f
RM    := rm -f
SED   := sed
TEST  := test

all:

sub_dir :=  $(call source-dir-to-binary-dir, util) $(call source-dir-to-binary-dir, tools) $(call source-dir-to-binary-dir, comm)
$(shell $(MKDIR) $(sub_dir))

local_src := $(wildcard *.cpp) $(wildcard util/*.cpp) $(wildcard tools/*.cpp) $(wildcard comm/*.cpp) 

$(eval $(call make-library,libtriplebit.so,$(local_src)))

$(eval $(call compile-rules))

.PHONY: all

all: libraries

.PHONY: libraries

libraries: $(libraries)

.PHONY: clean

clean:
	$(RM) -r $(BINARY_DIR)



ifneq "$(MAKECMDGOALS)" "clean"
  -include $(dependencies)
endif

