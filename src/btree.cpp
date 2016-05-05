/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
//#include <iostream>
//#include <memory>

//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	//first, check if the index file exists
	//if the file exists, open it
	//if not, create a new file
	//need to do somethi9ngn different for each 3 types of int, double and string
	std::cout << "input parameters\n";
	std::cout << "relationName: " << relationName << "\n";
	std::cout << "outIndexName: " << outIndexName << "\n";
	std::cout << "bufMgrIn: " << bufMgrIn << "\n";
	std::cout << "attrByteOffset: " << attrByteOffset << "\n";
	std::cout << "attrType: " << attrType << "\n";
	bufMgr = bufMgrIn;
	this->attrByteOffset = attrByteOffset;
	attributeType = attrType;

	//do the following depending on the attribute type of string, 
	//int or double
	//**********from the project spec*********
	//std::ostringstream idxStr;
	//idxStr << relationName << ’.’ << attrByteOffset;
	//std::string indexName = idxStr.str() ; // indexName is the name of the index file

	if (attributeType == 0){
		//0 = interger attribute type
		nodeOccupancy = INTARRAYNONLEAFSIZE;
		leafOccupancy = INTARRAYLEAFSIZE;
	} 
	else if (attributeType == 1){
		//1 = double attribute type
		nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
		leafOccupancy = DOUBLEARRAYLEAFSIZE;
	} 
	else if (attributeType == 2){
		//2 = string attribute type
		nodeOccupancy = STRINGARRAYNONLEAFSIZE;
		leafOccupancy = STRINGARRAYLEAFSIZE;	
	}
	else {
		//should never occur
		std::cout << "none of the attribute types matched";
		//throw exception
	}
	try {
		file = new BlobFile(outIndexName, false);
		//try to find the file?

	}catch (FileNotFoundException fnfe){
		//create the new file, which is declared in the header btree.h
		std::cout << "file not found exception \n";
		file = new BlobFile(outIndexName, true);
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	//flip the scanExecuting bool
	scanExecuting = false;
	//unpin btree pages that re pinned
	//for(int i = 0; i < bufMgr.bufPool.size(); i++)
	//{
	//	bufMgr->unpinpage(i);
	//}
	//flush the buffer and exit
	bufMgr->flushFile(file);
	delete file; // called to trigger the blobfile class's destructor
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	if(scanExecuting){
		endScan();
	}

	if( attributeType == INTEGER)
	{
		insertInteger(&key, rid);
	} 
	else if (attributeType == DOUBLE)
	{
		insertDouble(&key, rid);
	}
	else if (attributeType == STRING)
	{
		insertString(&key, rid);
	}
}


// ----------------------------------------------------------------------------
//BTreeIndex::insertInteger
//
// ---------------------------------------------------------------------------
const void BTreeIndex::insertInteger(const void *key, const RecordId rid) 
{
	RIDKeyPair<int> entry;
	entry.set(rid, *((int* )key));
}


// ----------------------------------------------------------------------------
//BTreeIndex::insertDouble
//
// ---------------------------------------------------------------------------
const void BTreeIndex::insertDouble(const void *key, const RecordId rid) 
{
	RIDKeyPair<int> entry;
	entry.set(rid, *((double* )key));


}

// ----------------------------------------------------------------------------
//BTreeIndex::insertString
//
// ---------------------------------------------------------------------------
const void BTreeIndex::insertString(const void *key, const RecordId rid) 
{
	RIDKeyPair<int> entry;
	entry.set(rid, *((char* )key));

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

//scan through leaf nodes until meets upper bound.

const void BTreeIndex::startScan(const void* lowValParm,
		const Operator lowOpParm,
		const void* highValParm,
		const Operator highOpParm)
{

	if((highOpParm != LTE && highOpParm != LT) || (lowOpParm != GTE && lowOpParm != GT)){
		throw BadOpcodesException();
	}

	lowOp = lowOpParm;
	highOp = highOpParm;

	if(lowValParm < highValParm){
		throw BadScanrangeException();
	}

	// checks if index scan has already started. Ends it if it has.
	if(scanExecuting){
		endScan();
	}

	scanExecuting = true;

	// if there are no nodes in the B+ tree
	if(rootPageNum == Page::INVALID_NUMBER){
		exit(1);
	}
	currentPageNum = rootPageNum;
	bufMgr->readPage(file, currentPageNum, currentPageData);

	if(attributeType == INTEGER){
		NonLeafNodeInt *currentNode = (NonLeafNodeInt *)currentPageData;
		lowValInt = *((int *)lowValParm);
		highValInt = *((int *)highValParm); 

		int i = 0;
		// loops until it finds the node that is one level above a leaf.
		while(currentNode->level != 1){
			i = 0;
			// finds beginning of range of nodes.
			while(currentNode->keyArray[i] < lowValInt && currentNode->pageNoArray[i + 1] != Page::INVALID_NUMBER && i < nodeOccupancy){
				i++;
			}
			// found first page:
			PageId nextNodePageNo = currentNode->pageNoArray[i];
			// reads page and saves it in currentPageData
			bufMgr->readPage(file, nextNodePageNo, currentPageData);
			bufMgr->unPinPage(file, currentPageNum, false);
			currentPageNum = nextNodePageNo;
			// moves to next node
			currentNode = (NonLeafNodeInt*) currentPageData;
		}
				
		// at level 1
		int j = 0;
		while(currentNode->keyArray[j] > lowValInt && currentNode->pageNoArray[j + 1] != Page::INVALID_NUMBER && j < nodeOccupancy){
			j++;
		}
		// found first page:
		PageId nextNodePageNo = currentNode->pageNoArray[j];
		// reads page and saves it in currentPageData
		bufMgr->readPage(file, nextNodePageNo, currentPageData);
		bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = nextNodePageNo;
		// moves to next node
		nextEntry = 0; // at leaf!

	}
	else if(attributeType == DOUBLE){

		NonLeafNodeDouble *currentNode = (NonLeafNodeDouble *)currentPageData;
		lowValDouble = *((double *)lowValParm);
		highValDouble = *((double *)highValParm); 

		int i = 0;
		// loops until it finds the node that is one level above a leaf.
		while(currentNode->level != 1){
			i = 0;
			// finds beginning of range of nodes.
			while(currentNode->keyArray[i] < lowValDouble && currentNode->pageNoArray[i + 1] != Page::INVALID_NUMBER && i < nodeOccupancy){
				i++;
			}
			// found first page:
			PageId nextNodePageNo = currentNode->pageNoArray[i];
			// reads page and saves it in currentPageData
			bufMgr->readPage(file, nextNodePageNo, currentPageData);
			bufMgr->unPinPage(file, currentPageNum, false);
			currentPageNum = nextNodePageNo;
			// moves to next node
			currentNode = (NonLeafNodeDouble*) currentPageData;
		}
				
		// at level 1
		int j = 0;
		while(currentNode->keyArray[j] > lowValDouble && currentNode->pageNoArray[j + 1] != Page::INVALID_NUMBER && j < nodeOccupancy){
			j++;
		}
		// found first page:
		PageId nextNodePageNo = currentNode->pageNoArray[j];
		// reads page and saves it in currentPageData
		bufMgr->readPage(file, nextNodePageNo, currentPageData);
		bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = nextNodePageNo;
		// moves to next node
		nextEntry = 0; // at leaf!

	}
	else if(attributeType == STRING){
		NonLeafNodeString *currentNode = (NonLeafNodeString *)currentPageData;
		lowValString = std::string((char *)lowValParm).substr(0,STRINGSIZE);
		highValString = std::string((char *)highValParm).substr(0,STRINGSIZE);

		int i = 0;
		// loops until it finds the node that is one level above a leaf.
		while(currentNode->level != 1){
			i = 0;
			// finds beginning of range of nodes.
			while(currentNode->keyArray[i] < lowValString && currentNode->pageNoArray[i + 1] != Page::INVALID_NUMBER && i < nodeOccupancy){
				i++;
			}
			// found first page:
			PageId nextNodePageNo = currentNode->pageNoArray[i];
			// reads page and saves it in currentPageData
			bufMgr->readPage(file, nextNodePageNo, currentPageData);
			bufMgr->unPinPage(file, currentPageNum, false);
			currentPageNum = nextNodePageNo;
			// moves to next node
			currentNode = (NonLeafNodeString*) currentPageData;
		}
				
		// at level 1
		int j = 0;
		while(currentNode->keyArray[j] > lowValString && currentNode->pageNoArray[j + 1] != Page::INVALID_NUMBER && j < nodeOccupancy){
			j++;
		}
		// found first page:
		PageId nextNodePageNo = currentNode->pageNoArray[j];
		// reads page and saves it in currentPageData
		bufMgr->readPage(file, nextNodePageNo, currentPageData);
		bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = nextNodePageNo;
		// moves to next node
		nextEntry = 0; // at leaf!

	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid)
{
	if(scanExecuting == false){
		throw ScanNotInitializedException();
	}
	bufMgr->readPage(file, currentPageNum, currentPageData);

	if(attributeType == INTEGER){
		bool cont = true;
		while(cont){
			LeafNodeInt *currentNode = (LeafNodeInt *)currentPageData;
			if((nextEntry >= nodeOccupancy) || (currentNode->ridArray[nextEntry].page_number == 0)){
				PageId nextPageNo = currentNode->rightSibPageNo;
				bufMgr->unPinPage(file, currentPageNum, false);
				if(nextPageNo == 0){ // scan is done:
					throw IndexScanCompletedException();
				}
				// scan is not done:
				currentPageNum = nextPageNo;
				bufMgr->readPage(file, currentPageNum, currentPageData);
				nextEntry = 0;
			}
			else if(((currentNode->keyArray[nextEntry] > lowValInt) && (lowOp == GT)) 
				|| ((currentNode->keyArray[nextEntry] <= lowValInt) && (lowOp == GTE))){
					nextEntry++;
			}
			else if(((currentNode->keyArray[nextEntry]) < highValInt && (highOp == LT)) 
				|| ((currentNode->keyArray[nextEntry] <= highValInt) && (highOp == LTE))){
				throw IndexScanCompletedException();
			}
			else{
				outRid = currentNode->ridArray[nextEntry];
				nextEntry;
				cont = false;
			}
		}
	}
	else if(attributeType == DOUBLE){
		bool cont = true;
		while(cont){
			LeafNodeDouble *currentNode = (LeafNodeDouble *)currentPageData;
			if((nextEntry >= nodeOccupancy) || (currentNode->ridArray[nextEntry].page_number == 0)){
				PageId nextPageNo = currentNode->rightSibPageNo;
				bufMgr->unPinPage(file, currentPageNum, false);
				if(nextPageNo == 0){ // scan is done:
					throw IndexScanCompletedException();
				}
				// scan is not done:
				currentPageNum = nextPageNo;
				bufMgr->readPage(file, currentPageNum, currentPageData);
				nextEntry = 0;
			}
			else if(((currentNode->keyArray[nextEntry] > lowValDouble) && (lowOp == GT)) 
				|| ((currentNode->keyArray[nextEntry] <= lowValDouble) && (lowOp == GTE))){
					nextEntry++;
			}
			else if(((currentNode->keyArray[nextEntry]) < highValDouble && (highOp == LT)) 
				|| ((currentNode->keyArray[nextEntry] <= highValDouble) && (highOp == LTE))){
				throw IndexScanCompletedException();
			}
			else{
				outRid = currentNode->ridArray[nextEntry];
				nextEntry;
				cont = false;
			}
		}
	}
	else if(attributeType == STRING){
		bool cont = true;
		while(cont){
			LeafNodeString *currentNode = (LeafNodeString *)currentPageData;
			if((nextEntry >= nodeOccupancy) || (currentNode->ridArray[nextEntry].page_number == 0)){
				PageId nextPageNo = currentNode->rightSibPageNo;
				bufMgr->unPinPage(file, currentPageNum, false);
				if(nextPageNo == 0){ // scan is done:
					throw IndexScanCompletedException();
				}
				// scan is not done:
				currentPageNum = nextPageNo;
				bufMgr->readPage(file, currentPageNum, currentPageData);
				nextEntry = 0;
			}
			else if(((currentNode->keyArray[nextEntry] > lowValString) && (lowOp == GT)) 
				|| ((currentNode->keyArray[nextEntry] <= lowValString) && (lowOp == GTE))){
					nextEntry++;
			}
			else if(((currentNode->keyArray[nextEntry]) < highValString && (highOp == LT)) 
				|| ((currentNode->keyArray[nextEntry] <= highValString) && (highOp == LTE))){
				throw IndexScanCompletedException();
			}
			else{
				outRid = currentNode->ridArray[nextEntry];
				nextEntry++;
				cont = false;
			}
		}
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan()
{
	if(scanExecuting == false){
		throw ScanNotInitializedException();
	}
	scanExecuting = false;
	if(currentPageNum != Page::INVALID_NUMBER){
		bufMgr->unPinPage(file, currentPageNum, false);
	}

}

}