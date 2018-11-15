#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "hfpage.h"
#include "buf.h"
#include "db.h"


// **********************************************************
// page class constructor

void HFPage::init(PageId pageNo)
{
// initialing the HFPage class
    nextPage = INVALID_PAGE; 
    prevPage = INVALID_PAGE; 
    curPage = pageNo;

    slotCnt = 0;
    usedPtr = MAX_SPACE-DPFIXED;
    freeSpace = MAX_SPACE-DPFIXED+sizeof(slot_t);
	
    slot->offset = usedPtr;
    slot->length = EMPTY_SLOT;
    //dumpPage();//////////////////////////for testing
}

// **********************************************************
// dump page utlity
void HFPage::dumpPage()
{
    int i;

    cout << "dumpPage, this: " << this << endl;
    cout << "curPage= " << curPage << ", nextPage=" << nextPage << endl;
    cout << "usedPtr=" << usedPtr << ",  freeSpace=" << freeSpace
         << ", slotCnt=" << slotCnt << endl;
   
    for (i=0; i < slotCnt; i++) {
        cout << "slot["<< i <<"].offset=" << slot[i].offset
             << ", slot["<< i << "].length=" << slot[i].length << endl;
    }
}

// **********************************************************
PageId HFPage::getPrevPage()
{
    // returns previous page number
    return prevPage;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{
    // set previous page number
    prevPage = pageNo;
}

// **********************************************************
PageId HFPage::getNextPage()
{
    // returns next page number
    return nextPage;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
  // sets next page number
    nextPage = pageNo;
}

// **********************************************************
// Add a new record to the page. Returns OK if everything went OK
// otherwise, returns DONE if sufficient space does not exist
// RID of the new record is returned via rid parameter.
Status HFPage::insertRecord(char* recPtr, int recLen, RID& rid)
{
    if (available_space() < recLen)  return DONE;  // no space available for this record

    int slotid = slotCnt, i; 
    for(i =0; i < slotCnt;i ++) { 
      if(slot[i].length == EMPTY_SLOT || slot[i].length == INVALID_SLOT) { //check if this slot is empty
	slotid = i; 
	break;       
      }  
    }
 
    if(i==slotCnt) {   //there is no empty slot; it has to add new slot
      slotCnt = slotCnt+1;
      freeSpace = freeSpace - sizeof(slot_t); //update the free space
    }

    //update the slot 
    slot[slotid].offset = usedPtr-recLen;
    slot[slotid].length = recLen;
    
    //update rid
    rid.slotNo=slotid;
    rid.pageNo=curPage;
    
    //update the used pointer and the freespace 
    usedPtr = usedPtr - recLen;
    freeSpace = freeSpace - recLen; 
    
    //copy the record into the data
    memcpy(data+usedPtr,recPtr,recLen); 
 

    //dumpPage();//////////////////////////for testing
    return OK;
}


// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.

Status HFPage::deleteRecord(const RID& rid)
{
    if (rid.pageNo != curPage || rid.slotNo <0 || rid.slotNo >= slotCnt)  return FAIL;

    int  slotid = rid.slotNo, i; 
    if (slot[slotid].length == EMPTY_SLOT || slot[slotid].length == INVALID_SLOT)   return FAIL;    

    for(i=slotid;i<slotCnt;i++)
       if(slot[i].length != EMPTY_SLOT && slot[i].length != INVALID_SLOT && slot[i].offset<slot[slotid].offset)
	  slot[i].offset += slot[slotid].length;

    memmove(data+usedPtr+slot[slotid].length, data+usedPtr, slot[slotid].offset-usedPtr);
    usedPtr += slot[slotid].length;   
    freeSpace += slot[slotid].length;
    slot[slotid].length = EMPTY_SLOT;

   while((slotid==(slotCnt-1)) && slotCnt>0 && (slot[slotid].length == EMPTY_SLOT || slot[slotid].length == INVALID_SLOT)) // compact/ release the end 
      {
       slotid--;
       slotCnt--;
       freeSpace += sizeof(slot_t);
      }
   //dumpPage();//////////////////////////for testing
    return OK;
}


// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
    int slotid = 0; 
 
    while( (slotid<slotCnt) && (slot[slotid].length == EMPTY_SLOT || slot[slotid].length == INVALID_SLOT) ) { 
	slotid++;      
      }  

    if (slotid == slotCnt) return DONE; // no record

    firstRid.pageNo = curPage;
    firstRid.slotNo = slotid;
    return OK;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
    if (curRid.pageNo != curPage || curRid.slotNo < 0 || curRid.slotNo >= slotCnt)   return FAIL;

    int slotid = curRid.slotNo+1; 
    while( (slotid<slotCnt) && (slot[slotid].length == EMPTY_SLOT || slot[slotid].length == INVALID_SLOT) ) { 
	slotid++;      
      }  

    if (slotid == slotCnt) return DONE; // no next record

    nextRid.pageNo = curPage;
    nextRid.slotNo = slotid;

    return OK;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
    if (rid.pageNo != curPage || rid.slotNo < 0 || rid.slotNo >= slotCnt || slot[rid.slotNo].length == INVALID_SLOT || slot[rid.slotNo].length == EMPTY_SLOT)
        return FAIL;

    recLen = slot[rid.slotNo].length;
    memcpy(recPtr, data+slot[rid.slotNo].offset, recLen);
    return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
    if (rid.pageNo != curPage || rid.slotNo < 0 || rid.slotNo >= slotCnt || slot[rid.slotNo].length == INVALID_SLOT || slot[rid.slotNo].length == EMPTY_SLOT)
        return FAIL;

    recPtr = data+slot[rid.slotNo].offset;
    recLen = slot[rid.slotNo].length;
    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    for(int i =0 ; i <slotCnt; i ++) { 
       if (slot[i].length == EMPTY_SLOT) { // There is an empty slot
          return freeSpace;  
       }
    }

  //if there is no empty slot; it has to add new slot
    if((unsigned)freeSpace < sizeof(slot_t)) return -1;
      else
        return freeSpace - sizeof(slot_t); 
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
    if(slotCnt == 0) return true;

    for(int i =0;i<slotCnt;i++){
        if(slot[i].length != INVALID_SLOT || slot[i].length != EMPTY_SLOT)
            return false;
    }
    return true;
}



