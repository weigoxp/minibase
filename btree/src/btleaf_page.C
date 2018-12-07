/*
 * btleaf_page.C - implementation of class BTLeafPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include "btleaf_page.h"
#include <stdio.h>
#include <string.h>

#define MAX_STRING 20

const char* BTLeafErrorMsgs[] = {
// OK,
// Insert Record Failed,
};
static error_string_table btree_table(BTLEAFPAGE, BTLeafErrorMsgs);
   
/*
 * Status BTLeafPage::insertRec(const void *key,
 *                             AttrType key_type,
 *                             RID dataRid,
 *                             RID& rid)
 *
 * Inserts a key, rid value into the leaf node. This is
 * accomplished by a call to SortedPage::insertRecord()
 * The function also sets up the recPtr field for the call
 * to SortedPage::insertRecord() 
 * 
 * Parameters:
 *   o key - the key value of the data record.
 *
 *   o key_type - the type of the key.
 * 
 *   o dataRid - the rid of the data record. This is
 *               stored on the leaf page along with the
 *               corresponding key value.
 *
 *   o rid - the rid of the inserted leaf record data entry.
 */

Status BTLeafPage::insertRec(const void *key,
                              AttrType key_type,
                              RID dataRid,
                              RID& rid)
{
    Status status; 
    KeyDataEntry key_data_entry;
    Datatype data;
    data.rid = dataRid;
    int entryLength;

    make_entry(&key_data_entry, key_type, key, LEAF, data, &entryLength);  // header file :bt.h, definition file: key.C
//cout<<"\n entryLength="<<entryLength;
    status = SortedPage::insertRecord (key_type, (char *) &key_data_entry, entryLength, rid);
    if (status != OK)    return MINIBASE_CHAIN_ERROR(BTLEAFPAGE, status);

  return OK;
}

/*
 *
 * Status BTLeafPage::get_data_rid(const void *key,
 *                                 AttrType key_type,
 *                                 RID & dataRid)
 *
 * This function performs a binary search to look for the
 * rid of the data record. (dataRid contains the RID of
 * the DATA record, NOT the rid of the data entry!)
 */

Status BTLeafPage::get_data_rid(void *key,
                                AttrType key_type,
                                RID & dataRid)
{
    Status status;
    RID currentRid, currentDataRid;
    Keytype currentKey;
    int comparedValue;

    status = get_first(currentRid, &currentKey, currentDataRid);
    if ( status != OK )   return status;    

    comparedValue = keyCompare(key, &currentKey, key_type);
    while(comparedValue!=0 && status==OK)
	{
	status = get_next(currentRid, &currentKey, currentDataRid);
	comparedValue = keyCompare(key, &currentKey, key_type);
	}

    if(comparedValue!=0) return RECNOTFOUND;
	
    dataRid.pageNo = currentDataRid.pageNo;
    dataRid.slotNo = currentDataRid.slotNo;	

  return OK;
}

/* 
 * Status BTLeafPage::get_first (const void *key, RID & dataRid)
 * Status BTLeafPage::get_next (const void *key, RID & dataRid)
 * 
 * These functions provide an
 * iterator interface to the records on a BTLeafPage.
 * get_first returns the first key, RID from the page,
 * while get_next returns the next key on the page.
 * These functions make calls to RecordPage::get_first() and
 * RecordPage::get_next(), and break the flat record into its
 * two components: namely, the key and datarid. 
 */
Status BTLeafPage::get_first (RID& rid,
                              void *key,
                              RID & dataRid)
{ 

	bool empty = HFPage::empty();
    if (empty)
        return NOMORERECS;

	slot_t *currSlot = this->slot;
	int i=0, offset, length, keySize;
	while(currSlot->offset==-1)
	{
		currSlot = (slot_t*)(data+i*sizeof(slot_t));
		i++;
	}
	rid.slotNo = i;
	rid.pageNo = curPage; 

	offset = currSlot->offset;
	length = currSlot->length;

	keySize = length - sizeof(RID);
	if(keySize == sizeof(int))
	{
		memcpy(key,data+offset,sizeof(int));
		memcpy(&dataRid,data+offset+keySize,sizeof(RID));
	}
	else
	{
		memcpy(key,data+offset,MAX_STRING);
		memcpy(&dataRid,data+offset+MAX_STRING,sizeof(RID));
	}
  return OK;
}

Status BTLeafPage::get_next (RID& rid,
                             void *key,
                             RID & dataRid)
{	 
	int  offset, length, keyLength, curSlotNum = rid.slotNo;
	slot_t *nextSlot;
	RID nextRid;

	if(curSlotNum+1 > this->slotCnt)  return NOMORERECS;
	
	nextSlot = (slot_t*)(data + curSlotNum*sizeof(slot_t));
	if(nextSlot->offset==-1) 
	{
		nextSlot = (slot_t*)(data + (curSlotNum+1)*sizeof(slot_t));
		curSlotNum++;
	}

	
	offset = nextSlot->offset;
	length = nextSlot->length;

	nextRid.pageNo = rid.pageNo;
	nextRid.slotNo = curSlotNum+1;
	rid = nextRid;


	keyLength = length - sizeof(RID);
	if(keyLength == sizeof(int))
	{
		memcpy(key,data+offset,sizeof(int));
		memcpy(&dataRid,data+offset+sizeof(int),sizeof(RID));
	} 
	else
	{
		memcpy(key,data+offset,MAX_STRING);
		memcpy(&dataRid,data+offset+MAX_STRING,sizeof(RID));
	}
  return OK;
}
