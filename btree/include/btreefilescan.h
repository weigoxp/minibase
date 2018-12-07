/*
 * btreefilescan.h
 *
 * sample header file
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 *
 */
 
#ifndef _BTREEFILESCAN_H
#define _BTREEFILESCAN_H
#include "btfile.h"
#include "bt.h"

// errors from this class should be defined in btfile.h
class BTreeFile;

class BTreeFileScan : public IndexFileScan {
public:
    friend class BTreeFile;

    BTreeFileScan(BTreeFile* file, const void *lo_key,
                         const void *hi_key, AttrType keyType);
    // get the next record
    Status get_next(RID & rid, void* keyptr);

    // delete the record currently scanned
    Status delete_current();

    int keysize(); // size of the key

    // destructor
    ~BTreeFileScan();
    //get firstleafpage
    Status getLowkeyLeafPage();

private:
    
	bool deleted;
	const void *lowkey;
	const void *highkey;
    BTreeFile *file;

    bool lowkeyisnull;
    BTLeafPage *curleaf;
    BTLeafPage *firstleafpage;

    PageId firstleafpageID;
    RID curleafRid;
	AttrType keyType;


};

#endif
