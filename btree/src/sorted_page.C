/*
 * sorted_page.C - implementation of class SortedPage
 *
 * Johannes Gehrke & Gideon Glass  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

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
  if (status != OK)
        return MINIBASE_CHAIN_ERROR(SORTEDPAGE, status);


  //rearrange the slot directory according to ordered key (insertion sort algorithm)
  int i, j, k;
  short temp;
  char *previousData, *newData;
  for (i = 1 ; i <= slotCnt - 1; i++) {
    if(slot[i].length!= EMPTY_SLOT && slot[i].length != INVALID_SLOT)
    {
          j = i-1;  
 	  k = i;   
          while ( j > 0)
          {
           if(slot[j].length!= EMPTY_SLOT && slot[j].length != INVALID_SLOT){
              previousData = data + slot[j].offset;
              newData = data + slot[k].offset;
	      if (keyCompare(previousData, newData, key_type) < 0){
      	          temp          = slot[j].offset;
      	          slot[j].offset   = slot[k].offset;
      	          slot[k].offset = temp;
	          k=j;
	          }
              }
           j--;
          }
    }
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
        if(slot[i].length != INVALID_SLOT || slot[i].length != EMPTY_SLOT)
            totalRec++;
    }
    return totalRec;
}
