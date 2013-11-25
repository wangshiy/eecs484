#include "catalog.h"
#include "query.h"
#include "index.h"
Status Operators::IndexSelect(const string& result,       // Name of the output relation
                              const int projCnt,          // Number of attributes in the projection
                              const AttrDesc projNames[], // Projection list (as AttrDesc)
                              const AttrDesc* attrDesc,   // Attribute in the selection predicate
                              const Operator op,          // Predicate operator
                              const void* attrValue,      // Pointer to the literal value in the predicate
                              const int reclen)           // Length of a tuple in the output relation
{
  cout << "Algorithm: Index Select" << endl;

  /* Your solution goes here 
  //Initialize index and Heap
  Status returnStatus;
  Index *index = new Index((*attrDesc).relName, (*attrDesc).attrOffset, (*attrDesc).attrLen, typeTrans(attrDesc->attrType), 0,status);
  if(returnStatus != OK){
    return returnStatus;
  }
  HeapFileScan *scan = new HeapFileScan((*attrDesc).relName, returnStatus);
  if(returnStatus != OK){
        return returnStatus;
  }

  HeapFile heapfile(result, returnStatus);
  if(returnStatus != OK){
	return returnStatus;
  }
//Start Scan --> if input = attrValue....

  if((returnStatus = index->startScan(attrValue)) == OK){
    RID rid;
    RID heaprid;
    Record record;

    while((status = index->scanNext(rid)) == OK){
        returnStatus = fileRec.getRandomRecord(rid, record);
        if(returnStatus != OK){
          return returnStatus;
        }
        Record new_record;
        new_record.length = 0;
        new_record.data = malloc(reclen);
        
        for (int j=0; j<projCnt; j++) {
          //creating output record
          memcpy((char *)new_record.data + new_record.length, (char *)record.data + projNames[j].attrOffset, projNames[j].attrLen);
          newRec.length +=  projNames[j].attrLen;                                                
        }

        returnStatus = heapfile.insertRecord(new_record,rid); //insert output into final heapfile relation
        if (returnStatus != OK) {
            free(new_record.data); //free the pointer in any possible exit
            return returnStatus;
        }
        free(new_record.data);    
    }
  }

  */
  return OK;
}

