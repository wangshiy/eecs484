#include "catalog.h"
#include "query.h"
#include "index.h"

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
Status Operators::Select(const string & result,      // name of the output relation
	                 	 const int projCnt,          // number of attributes in the projection
				         const attrInfo projNames[], // the list of projection attributes
				         const attrInfo *attr,       // attribute used inthe selection predicate 
		        		 const Operator op,         // predicate operation
		         		 const void *attrValue)     // literal value in the predicate
{

	AttrDesc predattrdesc;
	AttrDesc* projDesc = new AttrDesc[projCnt];
	int resultLen = 0;
	Status returnStatus;

	// Get projection attributes as attrDescs represented by projDesc
	for(int i = 0; i < projCnt; i++) {
		returnStatus = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projDesc[i]);
		if(returnStatus != OK){
			return returnStatus;
		}
		// Add up all attribute lengths in the relation
		resultLen += projDesc[i].attrLen;
	}

	if(attr != NULL) {
		const string relName(attr->relName);
		const string attrName(attr->attrName);

		// get attribute of the predicate
		returnStatus = attrCat->getInfo(attr->relName, attr->attrName, predattrdesc);

		if(returnStatus != OK){
			return returnStatus;
		}
	}
	// else{
	//	return ScanSelect(result, projCnt, projDesc, NULL, op, attrValue, resultLen);
	// }
	
	// Check if predicate has an index
	if(predattrdesc.indexed && op == EQ) {
		return IndexSelect(result, projCnt, projDesc, &predattrdesc, op, attrValue, resultLen);
	}
	else {
		return ScanSelect(result, projCnt, projDesc, &predattrdesc, op, attrValue, resultLen);
	}

	delete[] projDesc;
	return OK;
}
