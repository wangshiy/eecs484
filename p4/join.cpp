#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cmath>
#include <cstring>

#define MAX(a,b) ((a) > (b) ? (a) : (b))
#define DOUBLEERROR 1e-07

/*
 * Joins two relations
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Operators::Join(const string& result,           // Name of the output relation 
                       const int projCnt,              // Number of attributes in the projection
    	               const attrInfo projNames[],     // List of projection attributes
    	               const attrInfo* attr1,          // Left attr in the join predicate
    	               const Operator op,              // Predicate operator
    	               const attrInfo* attr2)          // Right attr in the join predicate
{
    /* Your solution goes here */
    Status returnStatus;
    int resultLen = 0;
    AttrDesc* projDesc = new AttrDesc[projCnt];
    AttrDesc attrdesc_1;
    AttrDesc attrdesc_2;

    // Get projection attributes as attrDescs represented by projDesc
    for(int i = 0; i < projCnt; i++) {
        returnStatus = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projDesc[i]);
        if(returnStatus != OK){
            return returnStatus;
        }
        // Add up all attribute lengths in the relation
        resultLen += projDesc[i].attrLen;
    }
    // Initialize Left attr in join predicate 
    returnStatus = attrCat->getInfo(attr1->relName, attr1->attrName, attrdesc_1);
    if(returnStatus != OK){
        return returnStatus;
    }
    // Initialize Right attr in join predicate 
    returnStatus = attrCat->getInfo(attr2->relName, attr2->attrName, attrdesc_2);
    if(returnStatus != OK){
        return returnStatus;
    }

    if(op == EQ){
        if(attrdesc_1.indexed){
            returnStatus = INL(result, projCnt, projDesc, attrdesc_1, op, attrdesc_2, resultLen);
            if(returnStatus != OK){
                return returnStatus;
            }
            else{
                return OK;
            }
        }
        if(attrdesc_2.indexed){
            returnStatus = INL(result, projCnt, projDesc, attrdesc_2, op, attrdesc_1, resultLen);
            if(returnStatus != OK){
                return returnStatus;
            }
            else{
                return OK;
            }
        }
        returnStatus = SMJ(result, projCnt, projDesc, attrdesc_1, op, attrdesc_2, resultLen);
        if(returnStatus != OK){
            return returnStatus;
        }
        else{
            return OK;
        }
    }
    else{
        returnStatus = SNL(result, projCnt, projDesc, attrdesc_1, op, attrdesc_2, resultLen);
        if(returnStatus != OK){
            return returnStatus;
        }
    }

	return OK;
}

// Function to compare two record based on the predicate. Returns 0 if the two attributes 
// are equal, a negative number if the left (attrDesc1) attribute is less that the right 
// attribute, otherwise this function returns a positive number.
int Operators::matchRec(const Record& outerRec,     // Left record
                        const Record& innerRec,     // Right record
                        const AttrDesc& attrDesc1,  // Left attribute in the predicate
                        const AttrDesc& attrDesc2)  // Right attribute in the predicate
{
    int tmpInt1, tmpInt2;
    double tmpFloat1, tmpFloat2, floatDiff;

    // Compare the attribute values using memcpy to avoid byte alignment issues
    switch(attrDesc1.attrType)
    {
        case INTEGER:
            memcpy(&tmpInt1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(int));
            memcpy(&tmpInt2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(int));
            return tmpInt1 - tmpInt2;

        case DOUBLE:
            memcpy(&tmpFloat1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(double));
            memcpy(&tmpFloat2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(double));
            floatDiff = tmpFloat1 - tmpFloat2;
            return (fabs(floatDiff) < DOUBLEERROR) ? 0 : floatDiff;

        case STRING:
            return strncmp(
                (char *) outerRec.data + attrDesc1.attrOffset, 
                (char *) innerRec.data + attrDesc2.attrOffset, 
                MAX(attrDesc1.attrLen, attrDesc2.attrLen));
    }

    return 0;
}