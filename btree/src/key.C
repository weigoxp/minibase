/*
 * key.C - implementation of <key,data> abstraction for BT*Page and 
 *         BTreeFile code.
 *
 * Gideon Glass & Johannes Gehrke  951016  CS564  UW-Madison
 * Edited by Young-K. Suh (yksuh@cs.arizona.edu) 03/27/14 CS560 Database Systems Implementation 
 */

#include <string.h>
#include <assert.h>

#include "bt.h"

#define MAX_STRING 20

/*
 * See bt.h for more comments on the functions defined below.
 */

/*
 * Reminder: keyCompare compares two keys, key1 and key2
 * Return values:
 *   - key1  < key2 : negative
 *   - key1 == key2 : 0
 *   - key1  > key2 : positive
 */
int keyCompare(const void *key1, const void *key2, AttrType t)
{
  // int type
  if(t==attrInteger){
      int *key11 = (int*) key1;
      int *key22 = (int*) key2;
      if(*key11< *key22) 
          return -1;
      else if(*key11> *key22)
          return  1;
      else 
          return 0;
  }
  // str type
  else if(t==attrString){
      char *key111 = (char*) key1;
      char *key222 = (char*) key2;

      return strcmp(key111,key222);
  }
      // others all same. we dont care.
  else
      return 0;
}

/*
 * make_entry: write a <key,data> pair to a blob of memory (*target) big
 * enough to hold it.  Return length of data in the blob via *pentry_len.
 *
 * Ensures that <data> part begins at an offset which is an even 
 * multiple of sizeof(PageNo) for alignment purposes.
 */
void make_entry(KeyDataEntry *target,
                AttrType key_type, const void *key,
                nodetype ndtype, Datatype data,
                int *pentry_len)
{
    //get keylen and datalen
    int keylen = get_key_length(key, key_type);
    int datalen =0;
    if(ndtype==INDEX)
        datalen=sizeof(PageId);

    if(ndtype==LEAF)
        datalen=sizeof(RID);
  // cpy key 
    memcpy((char *)target,key,keylen);
  // cpy data
    memcpy((char*)target+keylen,&data, datalen);

    *pentry_len = datalen+keylen;
}


/*
 * get_key_data: unpack a <key,data> pair into pointers to respective parts.
 * Needs a) memory chunk holding the pair (*psource) and, b) the length
 * of the data chunk (to calculate data start of the <data> part).
 */
void get_key_data(void *targetkey, Datatype *targetdata,
                  KeyDataEntry *psource, int entry_len, nodetype ndtype)
{
   //get data len first
    int datalen =0; 
    if(ndtype==INDEX)
        datalen=sizeof(PageId);

    else if(ndtype==LEAF)
        datalen=sizeof(RID);

    // key len;
    int keylen = entry_len - datalen;
 
    //cpy key and data
    memcpy(targetkey, &psource->key,keylen);
    memcpy((char*)targetdata, &psource->data,datalen);
}

/*
 * get_key_length: return key length in given key_type
 */
int get_key_length(const void *key, const AttrType key_type)
{
	if(key_type==attrInteger) return sizeof(int);

	else if(key_type==attrString)  return MAX_STRING;

}
  
/*
 * get_key_data_length: return (key+data) length in given key_type
 */   
int get_key_data_length(const void *key, const AttrType key_type, 
                        const nodetype ndtype)
{
    int len = get_key_length(key,key_type);

    if(ndtype==INDEX)
        return len+sizeof(PageId);

    if(ndtype==LEAF)
        return len+sizeof(RID);

    return -1;
}
