/*
 * btreefilescan.C - function members of class BTreeFileScan
 *
 * Spring 14 CS560 Database Systems Implementation
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 
 */

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btreefilescan.h"

/*
 * Note: BTreeFileScan uses the same errors as BTREE since its code basically 
 * BTREE things (traversing trees).
 */
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

BTreeFileScan::BTreeFileScan(BTreeFile *file,const void *lo_key, const void* hi_key, 
								AttrType keyType)
{
	//constructor needs to get the leaf node where low key resides.
	//get first leaf node then iterate to compare with lo_key
    Status status;
    RID dataRID;
    RID prevRID;

    status = getLowkeyLeafPage();

    if (status != OK)
        MINIBASE_CHAIN_ERROR(BTREE, status);

    curleaf = firstleafpage;

    if (firstleafpage != NULL) {
        status = MINIBASE_BM->pinPage(curleaf->page_no(), (Page *&) curleaf);
        if (status != OK) {
            MINIBASE_CHAIN_ERROR(BTREE, status);
            curleaf = NULL;
        }
        curleafRid.pageNo = INVALID_PAGE;
    }

    deleted = false;
    this->lowkey = lo_key;
    this->highkey = hi_key;
    this->keyType = keyType;
    this->file = file;

    lowkeyisnull = (lo_key==NULL);

    void* keyptr;

    if(keyType==attrInteger){
        keyptr = new int[1];
    }
    else if (keyType==attrString){
        keyptr = new char[MAX_KEY_SIZE1];
    }


    if(lowkeyisnull){
        curleaf->get_first(curleafRid, &keyptr, dataRID);

        while (status == OK && keyCompare(this->lowkey, &keyptr, keyType)> 0 ) {
            prevRID = curleafRid;
            status = curleaf->get_next(curleafRid, &keyptr, dataRID);
        }

        if ( status == OK )
            curleafRid = prevRID;
    }

}

BTreeFileScan::~BTreeFileScan()
{
    if (curleaf != NULL) {

        Status status = MINIBASE_BM->unpinPage(curleaf->page_no());
        if (status != OK) {
            MINIBASE_CHAIN_ERROR(BTREE, status);
        }
    }
}


Status BTreeFileScan::get_next(RID & rid, void* keyptr)
{
    Status status;
    RID dataRID;

	if (curleaf == NULL)
        return MINIBASE_FIRST_ERROR(BTREE, FAIL);

    if(keyCompare(this->lowkey, this->highkey, this->keyType)>0)
        return MINIBASE_FIRST_ERROR(BTREE, FAIL);

    // could be 1st move
    if (curleafRid.pageNo == INVALID_PAGE) 
        status = curleaf->get_first(curleafRid, keyptr, dataRID);
    // not 1st move
    else 
        status = curleaf->get_next(curleafRid, keyptr, dataRID);

  	// when the boundary is highkey.
	if (this->highkey != NULL && keyCompare(keyptr, this->highkey, this->keyType) > 0){

	    Status tmpstatus = MINIBASE_BM->unpinPage(curleaf->page_no());
	    if (tmpstatus != OK ) 
	        return MINIBASE_CHAIN_ERROR(BTREE, status);

	    // return
	    curleaf = NULL;
	    curleafRid.pageNo = INVALID_PAGE;

    	return DONE;
    }
    // what if reach the end.
    if (status == NOMORERECS) {

        PageId nextPageID = curleaf->getNextPage();
        Status tmpstatus = MINIBASE_BM->unpinPage(curleaf->page_no());
        if (tmpstatus != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, tmpstatus);

        if (nextPageID == INVALID_PAGE) {
            curleaf = NULL;
            curleafRid.pageNo = INVALID_PAGE;
            return DONE;
        }
    }

    this->deleted = false;
    rid = dataRID;
    return OK;
}

Status BTreeFileScan::delete_current()
{
  // put your code here
	if(deleted == true)
		return DONE;

	else{
		if(curleaf==NULL)
			return FAIL;

		Status status = curleaf->deleteRecord(curleafRid);
		if(status!=OK)
			return MINIBASE_CHAIN_ERROR(BTREE, status);			
	}

  return OK;
}

int BTreeFileScan::keysize() 
{
  return this->file->keysize();
}

Status BTreeFileScan::getLowkeyLeafPage(){

// traveral to 1st leaf page or lowkey leaf page.
    Status status;
    PageId pageNo = this->file->get_root();

    status = MINIBASE_BM->pinPage(firstleafpageID, (Page*&) firstleafpage);
    if (status != OK)
        MINIBASE_CHAIN_ERROR(BTREE, status);

    Page* page;
    RID tmpRid;
    PageId prevPageId;
    void* tmpKey;

    if(keyType==attrInteger){
        tmpKey = new int[1];
    }
    else if (keyType==attrString){
        tmpKey = new char[MAX_KEY_SIZE1];
    }



    status = MINIBASE_BM->pinPage(pageNo, page);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(BTREE, status);

    while (((SortedPage*) page)->get_type() != LEAF && pageNo != INVALID_PAGE) {
        PageId tmpPageNo;
        if (lowkeyisnull) {
            status = ((BTIndexPage*) page)->get_first(tmpRid, &tmpKey, tmpPageNo);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(BTREE, status);
        } else {
            status = ((BTIndexPage*) page)->get_page_no(tmpKey, this->keyType, tmpPageNo);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(BTREE, status);
       }

        status = MINIBASE_BM->unpinPage(pageNo, true);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(BTREE, status);
        prevPageId = pageNo;
        pageNo = tmpPageNo;

       if ( pageNo != INVALID_PAGE ) {
            status = MINIBASE_BM->pinPage(pageNo, page);
            if (status != OK)
                return MINIBASE_CHAIN_ERROR(BTREE, status);
      }
    }

    if ( pageNo == INVALID_PAGE ) 
        firstleafpageID = prevPageId;
    else {
        firstleafpageID = pageNo;


    }

    status = MINIBASE_BM->unpinPage(pageNo, true);
    if ( status != OK ) 
        return MINIBASE_CHAIN_ERROR(BTREE, status);
    return OK;
}