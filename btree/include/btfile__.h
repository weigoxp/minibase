/*
 * btfile.h
 *
 * sample header file
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */
 
#ifndef _BTREE_H
#define _BTREE_H

#include "btindex_page.h"
#include "btleaf_page.h"
#include "index.h"
#include "btreefilescan.h"
#include "bt.h"

#include <map>

// Define your error code for B+ tree here
// enum btErrCodes  {...}

class BTreeFile: public IndexFile
{
  public:
    BTreeFile(Status& status, const char *filename);
    // an index with given filename should already exist,
    // this opens it.
    
    BTreeFile(Status& status, const char *filename, const AttrType keytype, const int keysize);
    // if index exists, open it; else create it.
    
    ~BTreeFile();
    // closes index
    
    Status destroyFile();
    // destroy entire index file, including the header page and the file entry
    
    Status insert(const void *key, const RID rid);
    // insert <key,rid> into appropriate leaf page
    
    Status Delete(const void *key, const RID rid);
    // delete leaf entry <key,rid> from the appropriate leaf
    // you need not implement merging of pages when occupancy
    // falls below the minimum threshold (unless you want extra credit!)
    
    IndexFileScan *new_scan(const void *lo_key = NULL, const void *hi_key = NULL);
    // create a scan with given keys
    // Cases:
    //      (1) lo_key = NULL, hi_key = NULL
    //              scan the whole index
    //      (2) lo_key = NULL, hi_key!= NULL
    //              range scan from min to the hi_key
    //      (3) lo_key!= NULL, hi_key = NULL
    //              range scan from the lo_key to max
    //      (4) lo_key!= NULL, hi_key!= NULL, lo_key = hi_key
    //              exact match ( might not unique)
    //      (5) lo_key!= NULL, hi_key!= NULL, lo_key < hi_key
    //              range scan from lo_key to hi_key

    int keysize();

    map<int, char*> intRecords;
    map<char*, char*> stringRecords;

    // vital for starting any operation on the btree
    PageId get_root()
    {
    	return headerPage->pageId;
    }
    // vital for pinning, unpinning with buffer manager
    string get_fileName()
    {
    	return this->fileName;
    }
    
  private:
  	typedef struct {
        PageId pageId;
        AttrType keyType;
        int keySize;
        int treeHeight;
        void* minKeyVal;
        void* maxKeyVal;
    } HeaderPageInfo;

    HeaderPageInfo* headerPage;
    PageId headerPageId;
    string fileName;

    Status splitIndexPage(BTIndexPage* page, BTIndexPage* newPage);
    Status splitLeafPage(BTLeafPage* page, BTLeafPage* newLeafPage);

    char* create_key_data_record(const void *key,RID dataRId,int &recLen);
    char* create_key_index_record(const void *key,PageId pageNum,int &recLen);
    void insert(PageId &pageNum,const void *key,RID rid,KeyDataEntry *&child,PageId& splitPageId);
};

#endif
