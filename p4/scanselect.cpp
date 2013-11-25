#include "catalog.h"
#include "query.h"
#include "index.h"
#include <stdlib.h>
#include <cstring>
/* 
 * A simple scan select using a heap file scan
 */

Status Operators::ScanSelect(const string& result,       // Name of the output relation
                             const int projCnt,          // Number of attributes in the projection
                             const AttrDesc projNames[], // Projection list (as AttrDesc)
                             const AttrDesc* attrDesc,   // Attribute in the selection predicate
                             const Operator op,          // Predicate operator
                             const void* attrValue,      // Pointer to the literal value in the predicate
                             const int reclen)           // Length of a tuple in the result relation
{
  cout << "Algorithm: File Scan" << endl;
  
  /* Your solution goes here */
  //Initialize variables
	string relName(attrDesc->relName);
	Status returnStatus;
	RID outRid;
  RID heapRid;
	Record record;
	HeapFileScan* heapFileScan;

  //Check if attrDesc is NULL, and set params
  if(attrDesc == NULL) {
  	// Unfiltered scan
  	heapFileScan = new HeapFileScan(relName, returnStatus);
    if(returnStatus != OK){
      return returnStatus;
    }
	  //Begin Scan
		returnStatus = heapFileScan->startScan(0, 0, (Datatype)0, NULL, op);
    if(returnStatus != OK){
      return returnStatus;
    }
  }
  else {
  	// Filtered Scan
		heapFileScan = new HeapFileScan(relName, attrDesc->attrOffset, 
																	attrDesc->attrLen, (Datatype)attrDesc->attrType, 
																	(char*)attrValue, op, returnStatus);
  }

  //Continue Scan
  HeapFile* heapfile = new HeapFile(result, returnStatus);
	while( (returnStatus = heapFileScan->scanNext(outRid, record)) == OK) {
    Record record1;
    record1.data = malloc(reclen);
    record1.length = reclen;

    int offset = 0;
    for(int i = 0; i < projCnt; i++){
      memcpy((char *)record1.data + offset, (char *)record.data + projNames[i].attrOffset, projNames[i].attrLen);
      offset += projNames[i].attrLen;      
    }
    returnStatus = heapfile->insertRecord(record1, heapRid);
    if(returnStatus != OK){
      return returnStatus;
    }

  }

  //End scan, clean up
	heapFileScan->endScan();
	delete heapFileScan;
  delete heapfile;
  return OK;
}
