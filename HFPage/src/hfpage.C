#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include <iostream>

#include "hfpage.h"
#include "buf.h"
#include "db.h"
using namespace std;

// **********************************************************
// page class constructor
void HFPage::init(PageId pageNo)
{
  // fill in the body
    slotCnt = 0;
    usedPtr = MAX_SPACE - DPFIXED; //starts from very end.
    freeSpace = MAX_SPACE - DPFIXED; 
    
    prevPage = INVALID_PAGE;
    nextPage = INVALID_PAGE;
    curPage = pageNo;
    slot[0].offset = INVALID_SLOT;  // first slot dir
    slot[0].length = EMPTY_SLOT; // first slot dir
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
    // fill in the body
    return prevPage;
}

// **********************************************************
void HFPage::setPrevPage(PageId pageNo)
{
    // fill in the body
    prevPage = pageNo;
}

// **********************************************************
PageId HFPage::getNextPage()
{
    // fill in the body
    return nextPage;
}

// **********************************************************
void HFPage::setNextPage(PageId pageNo)
{
  // fill in the body
    nextPage = pageNo;
}

Status HFPage::insertRecord(char *recPtr, int recLen, RID &rid) {
    // check space -->
    // search if there's free slot. and update 1st freeslot offset
    // check space -->
    // search if there's free slot. and update 1st freeslot offset
    int slotPtr= -1;
      
    for(int i= 0; i< slotCnt+1;i++) {
        if( slot[i].length==-1)
            slotPtr=i;    
            break;    
}
    //if yes, just check space for record, 
    // if no then need space for record+ slot.
    if(slotPtr==-1&& freeSpace<recLen+ 4) {
        return DONE;
    }
    if(slotPtr!=-1 && freeSpace<recLen) {
        return DONE;
    }
    //if no empty slot, then creates a slot and points to it
    if(slotPtr==-1){
        slotCnt++;
        slotPtr = slotCnt;
        freeSpace-=4;
    }

    // now  modifies slot.
    //points to offset/usedptr
    slot[slotPtr].offset= usedPtr-recLen;
    slot[slotPtr].length= recLen;
    memcpy(data+slot[slotPtr].offset, recPtr, recLen);

   //     cout<< "offset:   "<<usedPtr <<endl; 
   //     cout<< "length:   "<<recLen <<endl; 
    //then copy record and return rid
    rid.pageNo = curPage;
    rid.slotNo = slotPtr;
    //update other info.

    usedPtr = usedPtr-recLen;
    freeSpace = freeSpace- recLen;

    return OK;
}



// **********************************************************
// Delete a record from a page. Returns OK if everything went okay.
// Compacts remaining records but leaves a hole in the slot array.
// Use memmove() rather than memcpy() as space may overlap.
Status HFPage::deleteRecord(const RID &rid) {
    // Make sure it's the right page
    if (rid.pageNo != curPage)
        return FAIL;
    // Grab the slot number
    int no = rid.slotNo;
    // Ensure that the slot is valid
    if (no < 0 || no > slotCnt)
        return FAIL;

    int slotNo= rid.slotNo;
    int curLen = slot[slotNo].length;
    int curOffset = slot[slotNo].offset;

    int len = curOffset- usedPtr;


    // clear slot/ update freespace
    freeSpace += curLen;
    slot[slotNo].offset = INVALID_SLOT;
    slot[slotNo].length = EMPTY_SLOT;

    // pack records tightly
    for (int i = 0; i < slotCnt+1; i++) {
        if (slot[i].length != EMPTY_SLOT && slot[i].offset < curOffset) {
            slot[i].offset += curLen;
        }
    }

    memmove(data + usedPtr+curLen,data + usedPtr, len);
    usedPtr+= curLen;
    // and we dont delete slot[0] !!
    for (int i = slotCnt; i > 0; i--) {
        if (slot[i].length != EMPTY_SLOT)
            break;
        slotCnt--;
        freeSpace+=4;
    }

    return OK;  
}

// **********************************************************
// returns RID of first record on page
//rid = (pageNo, slotNo)
Status HFPage::firstRecord(RID& firstRid)
{
    // fill in the body
    
/*
    firstRid.pageNo = curPage;
    for(int i =0; i<slotCnt+1;i++){
        if(slot[i].offset==999){
            firstRid.slotNo =i;
            return OK;
        }
    }
*/  
    for(int i = 0;i< slotCnt+1;i++){
        if(slot[i].offset!=INVALID_SLOT){
            firstRid.slotNo =i;
            firstRid.pageNo = curPage;
            return OK;
        }

    }
    return DONE;
}

// **********************************************************
// returns RID of next record on the page
// returns DONE if no more records exist on the page; otherwise OK
Status HFPage::nextRecord (RID curRid, RID& nextRid)
{   
    if(curRid.slotNo<0||curRid.slotNo > slotCnt) return FAIL;
    if(curRid.slotNo==INVALID_SLOT||curRid.pageNo!=curPage)
        return FAIL;

    for(int i = curRid.slotNo+1;i<slotCnt+1;i++){
        if(slot[i].offset != INVALID_SLOT){
            nextRid.slotNo = i;
            nextRid.pageNo = curPage;
            return OK;
        }
    }
    return DONE;

}

// **********************************************************
// returns length and copies out record with RID rid
Status HFPage::getRecord(RID rid, char* recPtr, int& recLen)
{   
    if(rid.slotNo<0) return FAIL;

    if (rid.pageNo != curPage)
        return FAIL;
    if (slot[rid.slotNo].length == EMPTY_SLOT)
        return FAIL;

        recLen= slot[rid.slotNo].length;
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
    // fill in the body


    recPtr= data + slot[rid.slotNo].offset;
    recLen= slot[rid.slotNo].length;
    return OK;
}

// **********************************************************
// Returns the amount of available space on the heap file page
int HFPage::available_space(void)
{
    // fill in the body
    return freeSpace;
}

// **********************************************************
// Returns 1 if the HFPage is empty, and 0 otherwise.
// It scans the slot directory looking for a non-empty slot.
bool HFPage::empty(void)
{
    for(int i =0;i<slotCnt+1;i++){
        if(slot[i].length !=EMPTY_SLOT)
            return false;
    }
    return true;
}




