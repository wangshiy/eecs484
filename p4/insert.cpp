#include "catalog.h"
#include "query.h"
#include "index.h"
#include "heapfile.h"
#include <string>
#include <stdlib.h>
#include <cstring>
/*
 * Inserts a record into the specified relation
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Updates::Insert(const string& relation,      // Name of the relation
                       const int attrCnt,           // Number of attributes specified in INSERT statement
                       const attrInfo attrList[])   // Value of attributes specified in INSERT statement
{
    /* Your solution goes here */
	AttrDesc* attrDescArray;
	int fullAttrCount = 0;
	int length = 0;
	Status returnStatus;
	RID outRid;
	Index* index;

	if(attrList == NULL){
		return ATTRTYPEMISMATCH;
	}


	/*
		Step 1: Get information about the attributes of the relation
	*/
	returnStatus = attrCat->getRelInfo(relation, fullAttrCount, attrDescArray);
		
	if(returnStatus != OK){
		return returnStatus;
	}

	/*
		Step 2: Verify that the input attribute count = relation attribute count
	if(fullAttrCount != attrCnt){
		return ATTRTYPEMISMATCH;
  }	*/

	/*
		Step 3: Verify that none of the values in the attribute list = NULL
	*/
	for(int i = 0; i < attrCnt; i++) {
		if(attrList[i].attrValue == NULL){
			return ATTRTYPEMISMATCH;
		}
	}

	/*
		Step 4: Determine the length of the record in # of bytes
	*/
	for(int i = 0; i < fullAttrCount; i++) {
		length += attrDescArray[i].attrLen;
	}

	/*
		Step 5: Create Record object with size of length from above
	*/

	Record record;
	record.data = malloc(length);
  record.length = length;


	/*
		Step 6/7: Search attrDescArray for a matching attrName; Insert index if index is found
	*/
		
	HeapFile* heapFile = new HeapFile(relation, returnStatus);

	for(int i = 0; i < attrCnt; i++){
		// Step 6
		for(int j = 0; j < fullAttrCount; j++) {
			if(strcmp(attrList[i].attrName, attrDescArray[j].attrName) == 0) {
				cout << attrList[i].attrName << endl;

				memcpy((char *)record.data + attrDescArray[j].attrOffset, attrList[i].attrValue, attrDescArray[j].attrLen);

				// Step 7  BROKEN BROKEN BROKEN BROKEN
				if(attrDescArray[j].indexed){
					index = new Index(attrDescArray[j].relName, attrDescArray[j].attrOffset, attrDescArray[j].attrLen, (Datatype)attrDescArray[j].attrType, 1, returnStatus);
					index->insertEntry(attrList[i].attrValue, outRid);
          			delete index;
				}
				//returnStatus = heapFile->insertRecord(record, outRid);

				if(returnStatus != OK) {
					return returnStatus;
				}
			}
		}
	}
	returnStatus = heapFile->insertRecord(record, outRid);
	if(returnStatus != OK) {
		return returnStatus;
	}

	/*
		Step 8: Insert into heapfile...segfaults happenin :(
	*/

	//returnStatus = heapFile->insertRecord(record, outRid);
	free(record.data);
	delete heapFile;
  return OK;
}
