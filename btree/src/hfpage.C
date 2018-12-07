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
    if(recLen > available_space())
    {
      return DONE;
    }
    if (this->freeSpace < recLen && this->slotCnt==0)
    {
      return DONE; // cannot fit
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

/*
    if (available_space() < recLen)  return DONE;  // no space available for this record
    if (this->freeSpace < recLen && this->slotCnt==0) return DONE;

    rid.pageNo = this->curPage;

    if(this->usedPtr == MAX_SPACE - DPFIXED)// there is no data; insert the first data
    {
        int offset = MAX_SPACE - DPFIXED - recLen;
        this->slot->offset = offset;
        this->slot->length = recLen;
        rid.slotNo = 0;

        this->usedPtr = offset;
        this->freeSpace = this->freeSpace - recLen;
        // copy the record to it's new allocated space
        memmove(data+offset,recPtr,recLen);
        return OK;
    }
    else
    {   // insert data
        int i=0, minOffset, offset; 
        slot_t *tmpSlot = this->slot;

        minOffset= tmpSlot->offset;

        while(i<=this->slotCnt)
            {
            if(tmpSlot->offset<minOffset && tmpSlot->offset!=-1)   minOffset = tmpSlot->offset;// count minimum offset
            if(tmpSlot->offset==-1)
            	{
                rid.slotNo=i;
                break;
            	}
            tmpSlot =(slot_t*)(data + i*sizeof(slot_t));
            i++;
            }

        tmpSlot->offset = minOffset - recLen;
        offset = tmpSlot->offset;

        if(i>this->slotCnt) {
            this->slotCnt += 1;
            this->freeSpace = this->freeSpace - recLen - sizeof(slot_t);
            rid.slotNo = i;
        }
        else{
            this->freeSpace = this->freeSpace - recLen;
        }
        tmpSlot->length = recLen;

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
  if (this->empty()||rid.pageNo != curPage || rid.slotNo <0 || rid.slotNo > slotCnt)  return FAIL;

  Status status;
  int slot = rid.slotNo, i, deletedLength, nextSlot, offset;
  slot_t *tmp, *curSlot;
  RID nextRecord, curRecord = rid;
  status = this->nextRecord(rid, nextRecord);

  curSlot = (slot_t *)(data+ (slot-1)*sizeof(slot_t));
  deletedLength = curSlot->length;
  curSlot->offset=-1;
  this->freeSpace = this->freeSpace + deletedLength;

  // compact the records in the file after delete; there will be no hole
  while(status==OK)
  {
    nextSlot = nextRecord.slotNo;
    tmp = (slot_t *)(data +(nextSlot-1)*sizeof(slot_t));
    offset = tmp->offset;

    memmove(data+offset+deletedLength,data+offset,tmp->length);
    curRecord = nextRecord;
    status = this->nextRecord(curRecord,nextRecord);
    tmp->offset = offset+deletedLength;
  }

  i=0;
  tmp = this->slot;
  int maxOffset = -1;
  while(i < this->slotCnt)
  {
    if(tmp->offset > maxOffset)  maxOffset = tmp->offset;
    tmp = (slot_t *)(data+i*sizeof(slot_t));
    i++;
  }

  // update  usedPtr
  if(maxOffset!=-1)   this->usedPtr = maxOffset;  
  else
  {// empty 
    this->usedPtr = MAX_SPACE - DPFIXED;
    this->slot->length = EMPTY_SLOT;
  }

  // update slotCnt
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
    if(empty()) return DONE;

    int slotid = 0; 
 
    while( (slotid<slotCnt) && (slot[slotid].offset ==-1)){//(slot[slotid].length == EMPTY_SLOT || slot[slotid].length == INVALID_SLOT) ) { 
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
  if (empty()||curRid.pageNo != curPage || curRid.slotNo < 0)   return FAIL; 
  
  int slotid = curRid.slotNo, curOffset, i=0, sum;
  slot_t *tmpSlot = this->slot;

  tmpSlot = (slot_t *)( data+ (slotid-1)*sizeof(slot_t));
  curOffset = tmpSlot->offset;


  tmpSlot = this->slot;
  while(i<=this->slotCnt)
  {
    sum = tmpSlot->offset + tmpSlot->length;
    if(sum==curOffset)
    {
      nextRid.pageNo = this->curPage;
      nextRid.slotNo = i;
      return OK;
    }
    tmpSlot = (slot_t *)(data+ i*sizeof(slot_t));
    i++;
  }
  return DONE;
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
