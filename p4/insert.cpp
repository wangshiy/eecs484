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
	int attrCount = 0;
	int length = 0;
	Status returnStatus;
	RID outRid;
	Index* index;

	for(int i = 0; i < attrCnt; i++){
		attrCat->getRelInfo(relation, attrCount, attrDescArray);
		
		// Search attrDescArray for a matching attrName
		for(int j = 0; j < attrCount; j++) {

			if(strcmp(attrList[i].attrName, attrDescArray[j].attrName) == 0) {
				length = attrDescArray[j].attrLen;
				break;
			}
		}

		// Create Record object with size of length from above
		Record record;
		record.data = malloc(length);	// SHOULD THIS BE +1??
		memcpy(record.data, attrList[i].attrValue, length);
		
		// Insert into heapfile
		HeapFile* heapFile = new HeapFile(relation, returnStatus);
		heapFile->insertRecord(record, outRid);

	}

	// Inset indexes
	for(int i = 0; i < attrCnt; i++) {
		// Search attrDescArray for a matching attrName
		for(int j = 0; j < attrCount; j++) {

			if(strcmp(attrList[i].attrName, attrDescArray[j].attrName) == 0) {
				// Insert index if index is found
				if(attrDescArray[j].indexed){
					index = new Index(attrDescArray[j].relName, attrDescArray[j].attrOffset, attrDescArray[j].attrLen, (Datatype)attrDescArray[j].attrType, 1, returnStatus);
					index->insertEntry(attrList[i].attrValue, outRid);
				}
			}
		}
	}
    return OK;
}
