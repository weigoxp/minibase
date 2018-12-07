#include <iostream>
#include <stdlib.h>
#include <memory.h>
#include <cassert>

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

    int slotid, i; 
    if(this->usedPtr == MAX_SPACE - DPFIXED)
    {slotid = 0;}
else{
    for(i =0; i < slotCnt;i ++) { 
      if(slot[i].length == EMPTY_SLOT || slot[i].length == INVALID_SLOT) { //check if this slot is empty
	slotid = i; 
	break;       
      }  
    }  
 
    if(i==slotCnt) {   //there is no empty slot; it has to add new slot
      slotCnt = slotCnt+1;
      freeSpace = freeSpace - sizeof(slot_t); //update the free space
slotid = slotCnt;
    }
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
    memmove(data+usedPtr,recPtr,recLen); 
 

    //dumpPage();//////////////////////////for testing
    return OK;

    // fill in the body
    /* Check if available free space can fit the record
     */
/*
    if(recLen > available_space())
    {
      return DONE;
    }

    rid.pageNo = this->curPage;

    // inserting the first record here
    if(this->usedPtr == MAX_SPACE - DPFIXED)
    {
        int offset = MAX_SPACE - DPFIXED - recLen;
        this->slot->offset = offset;
        this->slot->length = recLen;
        rid.slotNo = 0;

        this->usedPtr = offset;
        this->freeSpace = this->freeSpace - recLen;
        // copy the record to it's new allocated space
        memcpy(data+offset,recPtr,recLen);
        return OK;
    }
    else
    {   // here we look for the next available empty slot
        slot_t *tmp = this->slot;
        int j=0;
        int minOffset=tmp->offset;
        while(j <= this->slotCnt)
        {
            if(tmp->offset < minOffset && tmp->offset != -1)
            {
              minOffset = tmp->offset;
            }
            tmp = (slot_t *)(data+j*sizeof(slot_t));
            j++;
        }

        tmp = this->slot;
        j=0;


        while(j<=this->slotCnt)
        {
            if(tmp->offset==-1)
            {
                rid.slotNo=j;
                break;
            }
            tmp = (slot_t*)(data+j*sizeof(slot_t));
            j++;
        }

        tmp->offset = minOffset - recLen;
        int offset = tmp->offset;

        // we see if we are passed the compact etry in heap
        // and find the required freespace and
        // assign the slot the the new record
        if(j>this->slotCnt) {
            this->slotCnt += 1;
            this->freeSpace = this->freeSpace - recLen - sizeof(slot_t);
            rid.slotNo = j;
        }
        else{
            this->freeSpace = this->freeSpace - recLen;
        }
        tmp->length = recLen;

        // move the record to it's new allocated space
        memmove(data+offset,recPtr,recLen);
    }

    return OK;
*/
}

// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID& rid)
{
    // fill in the body
  int slot = rid.slotNo;
  // invalid page
  if(rid.pageNo!=this->curPage)
  {
    return FAIL;
  }

  if(this->empty())
  {
    return FAIL;
  }

  RID nextRecord, curRecord = rid;
  Status status = this->nextRecord(rid, nextRecord);

  if(slot > this->slotCnt || this->curPage != rid.pageNo)
  {
    return  FAIL;
  }

  slot_t *tmp = this->slot; 

  int j=0;
  // move to end to find empty slot
  while(j < this->slotCnt)
  {
    if(j==slot)
    {
      break;
    }
    tmp = (slot_t *)(data+j*sizeof(slot_t));
    j++;
  }


  //int deletedOffset = tmp->offset;
  int deletedLength = tmp->length;
  tmp->offset=-1;
  this->freeSpace = this->freeSpace + deletedLength;

  // if we have some next record, will compact them
  // this was not correctly implemented for P 1,2,3
  // even though the issue was not understandable till now
  while(status==OK)
  {
    int nextSlot = nextRecord.slotNo;
    tmp = (slot_t *)(data +(nextSlot-1)*sizeof(slot_t));
    int offset = tmp->offset;

    memmove(data+offset+deletedLength,data+offset,tmp->length);
    curRecord = nextRecord;
    status = this->nextRecord(curRecord,nextRecord);
    tmp->offset = offset+deletedLength;
  }

  j=0;
  tmp = this->slot;
  int maxOffset = -1;
  while(j < this->slotCnt)
  {
    if(tmp->offset > maxOffset)
    {
      maxOffset = tmp->offset;
    }
    tmp = (slot_t *)(data+j*sizeof(slot_t));
    j++;
  }

  // if this was not the only data, then we update Ptr
  // otherwise ptr will be max available
  if(maxOffset!=-1)
  {
    this->usedPtr = maxOffset;
  }
  else
  {
    this->usedPtr = MAX_SPACE - DPFIXED;
  }
  // fully empty, nothing there
  if(maxOffset==-1)
  {
    this->slot->length = EMPTY_SLOT;
  }

  // slot compaction to get the free space at the end of heap
  if(slot==this->slotCnt && this->slotCnt!=0)
  {
    this->slotCnt--;
    this->freeSpace = this->freeSpace + sizeof(slot_t);
  }
  return OK;
}

// **********************************************************
// returns RID of first record on page
Status HFPage::firstRecord(RID& firstRid)
{
    // fill in the body
    // no record found
    if(this->slotCnt == 0)
    {
      return DONE;
    }

  slot_t *tmp = this->slot;

    while (tmp!=this->slot+this->slotCnt && (tmp->length == EMPTY_SLOT || tmp->length == EMPTY_SLOT) )
    {
      tmp++;
    }

    if (tmp == this->slot + this->slotCnt)
    {
        return DONE;
    }

    firstRid.pageNo = this->curPage;
    firstRid.slotNo = tmp - this->slot;

    return OK;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{
    // fill in the body
    // record validity check
    // has to be at least one record to find the next record
  if(usedPtr == MAX_SPACE - DPFIXED)
  {
    return  FAIL;
  }
  if(this->empty())
  {
    return FAIL;
  }
  if(curRid.pageNo!=this->curPage)
  {
    return FAIL;
  }

  int curSlot = curRid.slotNo;
  int curOffset;
  slot_t *tmp = this->slot;
  int j=0;

  //move to current slot
  while(j<=this->slotCnt)
  {
    if(j==curSlot && tmp->offset!=-1)
    {
      curOffset = tmp->offset;
      break;
    }
    tmp = (slot_t *)(data+j*sizeof(slot_t));
    j++;
  }

  j=0;
  tmp = this->slot;
  bool recordFound = false;
  // find next record by summing offsets and lengths
  // and equating to current record's offset
  while(j<=this->slotCnt)
  {
    int sum = tmp->offset + tmp->length;
    if(sum==curOffset)
    {
      nextRid.pageNo = this->curPage;
      nextRid.slotNo = j;
      recordFound = true;
      break;
    }
    tmp = (slot_t *)(data+j*sizeof(slot_t));
    j++;
  }
  if(!recordFound)
  {
    return DONE;
  }
  return OK;
}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{
    // fill in the body
    if (rid.slotNo < 0)
  {
    return FAIL;
  }
  // misplaced record
  if (rid.slotNo >= this->slotCnt)
  {
    return FAIL;
  }
  // record in wrong page
  if (rid.pageNo != this->curPage)
  {
    return FAIL;
  }
  // invalid record with no field
  if (this->slot[rid.slotNo].length == EMPTY_SLOT || this->slot[rid.slotNo].length == INVALID_SLOT)
  {
    return FAIL;
  }

  recLen = this->slot[rid.slotNo].length;
    memcpy(recPtr, &(this->data[slot[rid.slotNo].offset]), recLen);

    return OK;
}

// **********************************************************
// returns length and pointer to record with RID rid.  The difference
// between this and getRecord is that getRecord copies out the record
// into recPtr, while this function returns a pointer to the record
// in recPtr.
Status HFPage::returnRecord(RID rid, char*& recPtr, int& recLen)
{
    // fill in the body
    // no record
  if (rid.slotNo < 0)
  {
    return FAIL;
  }
  // misplaced record
  if (rid.slotNo >= this->slotCnt)
  {
    return FAIL;
  }
  // record in wrong page
  if (rid.pageNo != this->curPage)
  {
    return FAIL;
  }
  // invalid record with no field
  if (this->slot[rid.slotNo].length == EMPTY_SLOT && this->slot[rid.slotNo].length == INVALID_SLOT)
  {
    return FAIL;
  }

  // update record pointer and return the record
  recPtr = &(this->data[slot[rid.slotNo].offset]);
  recLen = this->slot[rid.slotNo].length;

    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    // fill in the body
    /* checks the available free space and returns
     */
    slot_t *tmp = this->slot;
    while( tmp != this->slot + this->slotCnt )
    {
      if(tmp->length == EMPTY_SLOT || tmp->length == INVALID_SLOT)
      {
        // current pointer already at the end of data[]
        return this->freeSpace;
      }
      tmp++;
    }

    if( (unsigned)this->freeSpace < sizeof(slot_t) )
  {
    // can't fit in new slot anymore
    return 0;
  }
  else {
    return this->freeSpace - sizeof(slot_t);
  }
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
    // fill in the body
    if(this->slotCnt == 0)
    {
      // some record in some slot
      return true;
    }
    return false;
}
