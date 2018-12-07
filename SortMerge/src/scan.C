/*
 * implementation of class Scan for HeapFile project.
 * $Id: scan.C,v 1.1 1997/01/02 12:46:42 flisakow Exp $
 */

#include <stdio.h>
#include <stdlib.h>

#include "heapfile.h"
#include "scan.h"
#include "hfpage.h"
#include "buf.h"
#include "db.h"

// *******************************************
// The constructor pins the first page in the file
// and initializes its private data members from the private data members from hf
Scan::Scan (HeapFile *hf, Status& status)
{
  status = init(hf);
}

// *******************************************
// The deconstructor unpin all pages.
Scan::~Scan()
{
   reset();
}

// *******************************************
// Retrieve the next record in a sequential scan.
// Also returns the RID of the retrieved record.
Status Scan::getNext(RID& rid, char *recPtr, int& recLen)
{
    Status status;
    if (scanIsDone)
        return DONE;

    if (nxtUserStatus != OK) {  // need to look into next data page 
        status = nextDataPage(); 
        if (status == DONE) {
            return DONE;
        } else if (status != OK) {
            return MINIBASE_CHAIN_ERROR(SCAN, status);
        }
    }

    // Fetch the record
    status = dataPage->getRecord(userRid, recPtr, recLen);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SCAN, status);

    rid = userRid;

    // Move userRid to the next location, and put that result into the nxtUserStatus
    // variable indicating if there is another record
    nxtUserStatus = dataPage->nextRecord(userRid, userRid);

    return OK;
}

// *******************************************
// Do all the constructor work.
Status Scan::init(HeapFile *hf)
{
    _hf = hf; 
    scanIsDone = false; 
    Status status = firstDataPage();
    scanIsDone = hf->getRecCnt() == 0;
    return status;
}

// *******************************************
// Reset everything and unpin all pages.
Status Scan::reset()
{
    Status status;
    if (dirPageId != INVALID_PAGE) {
        status = MINIBASE_BM->unpinPage(dirPageId);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(SCAN, status);
        dirPageId = _hf->firstDirPageId;
    }
    dirPage = NULL;

    if (dataPageId != INVALID_PAGE) {
        status = MINIBASE_BM->unpinPage(dataPageId);
        if (status != OK)
            return MINIBASE_CHAIN_ERROR(SCAN, status);

        dataPageId = INVALID_PAGE;
    }
    dataPage = NULL;

    scanIsDone = false;
    nxtUserStatus = DONE;
  return OK;
}

// *******************************************
// Copy data about first page in the file.
Status Scan::firstDataPage()
{
    Status status;
    scanIsDone = false;
    dirPageId = _hf->firstDirPageId;

    status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SCAN, status);

    status = dirPage->firstRecord(dataPageRid);
    if (status != OK) {// no record in firstDirPage and so in heap file
   
        if (status == DONE) return OK;
          else return MINIBASE_CHAIN_ERROR(SCAN, status);
    }

    DataPageInfo *dataPageInfo = new DataPageInfo();
    int dirPageRecLength;
    status = dirPage->getRecord(dataPageRid, (char *) dataPageInfo, dirPageRecLength);
    if ( status != OK ) 
      return MINIBASE_CHAIN_ERROR(SCAN, status);

    dataPageId = dataPageInfo->pageId;

    status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SCAN, status);

    status = dataPage->firstRecord(userRid);
    if (status != OK)
       { if (status == DONE) return DONE;
           else return MINIBASE_CHAIN_ERROR(SCAN, status);
       }

    nxtUserStatus = OK;

    return OK;
}

// *******************************************
// Retrieve the next data page.
Status Scan::nextDataPage(){
    Status status, dirPageStatus;
    RID nextDirPageRID;
    DataPageInfo *dataPageInfo = new DataPageInfo();
    int dirPageRecLength;

    // Retrieve the next dataPageinfo RID from the directory
    status = dirPage->nextRecord(dataPageRid, nextDirPageRID);
    if(status == FAIL) return MINIBASE_CHAIN_ERROR(SCAN, status);
      else if(status== DONE)  // no more record in this directory page
	     {
	     dirPageStatus = nextDirPage(); //look into next directorypage
	     if(dirPageStatus == OK) status = dirPage->firstRecord(nextDirPageRID);//get first record from directory page
		else if(dirPageStatus == DONE) { reset();  return DONE; }// no more record
		   else return MINIBASE_CHAIN_ERROR(SCAN, dirPageStatus);

	     }

    // got next dataPageRid
    dataPageRid = nextDirPageRID;

    // get DataPageInfo from the directory page
    dirPageStatus = dirPage->getRecord(dataPageRid, (char *) dataPageInfo, dirPageRecLength);
    if ( dirPageStatus != OK ) {
      status = MINIBASE_BM->unpinPage(dataPageId);
      if (status != OK)
          return MINIBASE_CHAIN_ERROR(SCAN, status);
      return MINIBASE_CHAIN_ERROR(SCAN, dirPageStatus);
    }

    // Unpin the current data page
    status = MINIBASE_BM->unpinPage(dataPageId);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SCAN, status);
    //dataPageId = INVALID_PAGE;

    // Set new dataPageId and pin
    dataPageId = dataPageInfo->pageId;
    status = MINIBASE_BM->pinPage(dataPageId, (Page *&) dataPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SCAN, status);

    dataPage->firstRecord(userRid);
    nxtUserStatus = OK;

  return OK;
}

// *******************************************
// Retrieve the next directory page.
Status Scan::nextDirPage() {
    Status status;
    PageId preDirPageId = dirPageId;
    dirPageId = dirPage->getNextPage();
    
    status = MINIBASE_BM->unpinPage(preDirPageId);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SCAN, status);

    if (dirPageId == INVALID_PAGE) // no more pages in the directory
        return DONE; 

    status = MINIBASE_BM->pinPage(dirPageId, (Page *&) dirPage);
    if (status != OK)
        return MINIBASE_CHAIN_ERROR(SCAN, status);
  return OK;
}

// *******************************************

Status Scan::mvNext(RID& rid){
    int recLen;
    char recPtr[10];

    return getNext(rid, recPtr, recLen);


}
// *******************************************
Status Scan::position(RID rid){
    reset();
    firstDataPage();

    RID tmpRid;
    char *tmpRec = new char[1000];
    int tmpLen;
    while (userRid != rid) {
        Status status = getNext(tmpRid, tmpRec, tmpLen);
        if (status != OK)
            return status;
    }

    delete tmpRec;
    return OK;
}
