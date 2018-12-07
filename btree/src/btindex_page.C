/*
 * btindex_page.C - implementation of class BTIndexPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btindex_page.h"
#include <stdio.h>
#include <string.h>

// Define your Error Messge here
const char* BTIndexErrorMsgs[] = {
  //Possbile error messages, 
  //OK,
  //Record Insertion Failure,
};

static error_string_table btree_table(BTINDEXPAGE, BTIndexErrorMsgs);

/* we try to see if we can insert a record in the leaf index
 * if the status is overflow (!OK) it will force a node split
 */
Status BTIndexPage::insertKey (const void *key,
                               AttrType key_type,
                               PageId pageNo,
                               RID& rid)
{
    Status status; 
    KeyDataEntry key_data_entry;
    Datatype data;
    data.pageNo = pageNo;
    int entryLength;

    make_entry(&key_data_entry, key_type, key, INDEX, data, &entryLength);  // header file :bt.h, definition file: key.C

    status = SortedPage::insertRecord (key_type, (char *) &key_data_entry, entryLength, rid);
    if (status != OK)    return MINIBASE_CHAIN_ERROR(BTINDEXPAGE, status);
  return OK;
}

/* we delete a record with key and RID
 * but this time the hfpage is designed to keep
 * the records sorted so we have sorted
 * entries in the btree index pages
 */
Status BTIndexPage::deleteKey (const void *key, AttrType key_type, RID& curRid)
{

    Status status;
    RID currentRid;
    PageId currentPageId;
    Keytype currentKey;
    int comparedValue;

    status = get_first(currentRid, &currentKey, currentPageId);
    if ( status != OK )   return status;    

    comparedValue = keyCompare(key, &currentKey, key_type);
    while(comparedValue>0 && status==OK)
	{
	status = get_next(currentRid, &currentKey, currentPageId);
	comparedValue = keyCompare(key, &currentKey, key_type);
	}

    if(status!=OK) return RECNOTFOUND;
	
    curRid = currentRid;

    status = deleteRecord(curRid);

  return OK;
}

Status BTIndexPage::get_page_no(const void *key,
                                AttrType key_type,
                                PageId & pageNo)
{
    Status status;
    int prevCompare = 0,compare =0;
    RID rid;
    PageId pageId, prevPageId;
    void *k;

    if(key_type == attrInteger)	k=new int[1];	
	else	k = new char[MAX_KEY_SIZE1];

    status = get_first(rid,k,pageId);
	
    if(keyCompare(key, k,key_type)<0)
	{
	 pageNo= pageId; return OK;
	}
	
	
    while(status == OK)
	{
	prevCompare = compare;
	prevPageId = pageId;
	compare = keyCompare(key,k,key_type);
	if(compare == 0)
		{
		pageNo= pageId; return OK;
		}
	if(prevCompare*compare<0) //  prevCompare>0 and  compare<0
		{
		pageNo= prevPageId; return OK;
		}
	status = get_next(rid,k,pageId);
	}

    pageNo= prevPageId;
    return OK;
}

/* Required when we do some operation on a page
 * returns the leftmost rid.
 */
Status BTIndexPage::get_first(RID& rid,
                              void *key,
                              PageId & pageNo)
{
	slot_t *currSlot = this->slot;
	int i=0, offset, length, keySize;
	while(currSlot->offset==-1)
	{
		currSlot = (slot_t *)(data+i*sizeof(slot_t));
		i++;
	}
	rid.slotNo = i;
	rid.pageNo = curPage;

	offset = currSlot->offset;
	length = currSlot->length;

	keySize = length - sizeof(PageId);
	if(keySize == sizeof(int))
	{
		memcpy(key,data+offset,sizeof(int));
		memcpy(&pageNo,data+offset+keySize,sizeof(PageId));
	}
	else
	{
		memcpy(key,data+offset,keySize);
		memcpy(&pageNo,data+offset+keySize,sizeof(PageId));
	}
  return OK;
}

Status BTIndexPage::get_next(RID& rid, void *key, PageId & pageNo)
{
	int offset, length, keySize, currSlotNum = rid.slotNo;
	slot_t *nextSlot;
	RID nextRid;

	if(currSlotNum+1 > this->slotCnt)	return NOMORERECS;

	nextSlot = (slot_t *)(data+ currSlotNum*sizeof(slot_t));
	offset = nextSlot->offset;
	length = nextSlot->length;

	nextRid.pageNo = rid.pageNo;
	nextRid.slotNo = rid.slotNo+1;
	rid = nextRid;

	keySize = length - sizeof(PageId);
	if(keySize == sizeof(int))
	{
		memcpy(key,data+offset,sizeof(int));
		memcpy(&pageNo,data+offset+keySize,sizeof(PageId));
	}
	else
	{
		memcpy(key,data+offset,keySize);
		memcpy(&pageNo,data+offset+keySize,sizeof(PageId));
	}
  return OK;
}
