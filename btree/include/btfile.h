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
#include <vector>
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

    vector<int> intValues;
    vector<string> stringValues;
    map<int, char*> intRecords;
    map<char*, char*> stringRecords;
    vector<int> getIntVals()
    {
        return intValues;
    }
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
        int numLevels;
    } HeaderPage;

    HeaderPage* headerPage;
    PageId headerPageId;
    string fileName;

    HeaderPage* get_header_page();

    bool isRoot(PageId);

    vector<BTLeafPage> leafPages;
    Status splitIndexPage(BTIndexPage* page, BTIndexPage* left);
    Status splitLeafPage(BTLeafPage* page, BTLeafPage* other);
    Status create_parent(BTIndexPage *page,char *left,char *right);

    char* create_key_data_record(const void *key,RID dataRId,int &recLen);
    char* create_key_index_record(const void *key,PageId pageNum,int &recLen);
    void insert(PageId &pageNum,const void *key,RID rid,KeyDataEntry *&child,PageId& splitPageId);
    
    Status get_leaf_page_for_insertion(const void *key, BTIndexPage *parent,PageId &insertionPage,void *k);
    short get_page_type(PageId);
    PageId get_page_no(BTIndexPage *page,const void *key,AttrType type);
};

#endif
