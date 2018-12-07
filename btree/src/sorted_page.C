/*
 * sorted_page.C - implementation of class SortedPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include <vector>
#include <algorithm>

#include "sorted_page.h"
#include "btindex_page.h"
#include "btleaf_page.h"

const char* SortedPage::Errors[SortedPage::NR_ERRORS] = {
  //OK,
  //Insert Record Failed (SortedPage::insertRecord),
  //Delete Record Failed (SortedPage::deleteRecord,
};


/*
 *  Status SortedPage::insertRecord(AttrType key_type, 
 *                                  char *recPtr,
 *                                    int recLen, RID& rid)
 *
 * Performs a sorted insertion of a record on an record page. The records are
 * sorted in increasing key order.
 * Only the  slot  directory is  rearranged.  The  data records remain in
 * the same positions on the  page.
 *  Parameters:
 *    o key_type - the type of the key.
 *    o recPtr points to the actual record that will be placed on the page
 *            (So, recPtr is the combination of the key and the other data
 *       value(s)).
 *    o recLen is the length of the record to be inserted.
 *    o rid is the record id of the record inserted.
 */
 
Status SortedPage::insertRecord (AttrType key_type,
                                 char * recPtr,
                                 int recLen,
                                 RID& rid)
{
  Status status;

  if(key_type!=attrString && key_type!=attrInteger)      //check key_type
	return MINIBASE_FIRST_ERROR(SORTEDPAGE, ATTRNOTFOUND);

  status = HFPage::insertRecord(recPtr, recLen, rid);    // insert the record
  if (status != OK)  return MINIBASE_CHAIN_ERROR(SORTEDPAGE, status);


  int i=0, j=0, offset, length;
  vector<SlotDataInfo> slotsInfo;
  slot_t *tmpSlot = this->slot;
	 
  while(i < numberOfRecords())
	{
		SlotDataInfo slotData;
		slotData.slotNo = i;

		offset = tmpSlot->offset;
		length = tmpSlot->length;
		slotData.slotLength = length;
		slotData.slotOffset = offset;
		if(key_type == attrString)
		{
			slotData.type = attrString;
			if(recLen > MAX_KEY_SIZE1)
			{
				char *rec = new char[MAX_KEY_SIZE1];
				memcpy(rec,data+offset,MAX_KEY_SIZE1);
				slotData.stringData = rec;
			}
			else
			{
				char *rec = new char[recLen];
				memcpy(rec,data+offset,recLen);
				slotData.stringData = rec;
			}
		}
		else
		{
			slotData.type = attrInteger;
			memcpy(slotData.intData,data+offset,sizeof(int));
		}
		slotsInfo.push_back(slotData);
		tmpSlot = (slot_t*)(data+j*sizeof(slot_t));

		i++;
		j++;
	}
	rid.slotNo = this->slotCnt;

	// here we perform the sort
	std::sort(slotsInfo.begin(),slotsInfo.end());

	tmpSlot =this->slot;
	i=0;
	j=0;
	while(i<(int)slotsInfo.size())
	{
		if(tmpSlot->offset==-1) break;

		tmpSlot->offset = slotsInfo.at(i).slotOffset;
		tmpSlot->length = slotsInfo.at(i).slotLength;
		i++;
		tmpSlot = (slot_t*)(data+j*sizeof(slot_t));
		j++;
	}

	return OK;
}


/*
 * Status SortedPage::deleteRecord (const RID& rid)
 *
 * Deletes a record from a sorted record page. It just calls
 * HFPage::deleteRecord().
 */

Status SortedPage::deleteRecord (const RID& rid)
{
  return HFPage::deleteRecord(rid);
}

int SortedPage::numberOfRecords()
{
    int totalRec = 0;
    for(int i =0; i<slotCnt; i++){
        if(slot[i].offset !=-1)//(slot[i].length != INVALID_SLOT && slot[i].length != EMPTY_SLOT)
            totalRec++;
        else break;
    }
    return totalRec;
}
