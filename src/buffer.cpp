/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

    BufMgr::BufMgr(std::uint32_t bufs)
            : numBufs(bufs) {
        bufDescTable = new BufDesc[bufs];

        for (FrameId i = 0; i < bufs; i++)
        {
            bufDescTable[i].frameNo = i;
            bufDescTable[i].valid = false;
        }

        bufPool = new Page[bufs];

        int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
        hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

        clockHand = bufs - 1;
    }


    BufMgr::~BufMgr() {
        FrameId i;
        BufDesc* tmpBufDesc;
        for(i = 0; i < numBufs; i++){
            tmpBufDesc = &bufDescTable[i];
            if(tmpBufDesc->valid == true && tmpBufDesc->dirty == true){
                //Page newPage = tmpBufDesc->file->allocatePage();
                tmpBufDesc->file->writePage(bufPool[i]);
            }
        }
        delete[] bufDescTable;
        delete[] bufPool;
        delete hashTable;
    }

    void BufMgr::advanceClock()
    {
        clockHand++;
        clockHand = clockHand % numBufs;
    }

    void BufMgr::allocBuf(FrameId & frame)
    {
        FrameId numFrame = 0;
        bool isAvailable = false;
        do{
            advanceClock();
            numFrame++;
            if(bufDescTable[clockHand].valid == false){
                break;
            }
            if(bufDescTable[clockHand].refbit == true){
                bufDescTable[clockHand].refbit = false;
                bufStats.accesses++;
            }
            else{
                if(bufDescTable[clockHand].pinCnt == 0){
                    hashTable->remove(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
                    isAvailable = true;
                    break;
                }
            }
        }while(numFrame < 2*numBufs);//finish the second round and find no frame

        //clockHand has scanned two times and find no frame, throw an exception
        if(numFrame >= 2*numBufs && isAvailable==false ){throw BufferExceededException();}

        //std::cout<< "clockHand : "<<clockHand<<std::endl;

        //flush page to disk when dirty bit is true and update states
        if(bufDescTable[clockHand].dirty == true){
            //Page newPage = bufDescTable[clockHand].file->allocatePage();
            bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
            bufStats.diskwrites++;
        }

        bufDescTable[clockHand].Clear();

        frame = clockHand;
        return;
    }


    void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
    {
        //set candidate returned frameNo
        FrameId frameNo = 0;
        //case 2:page in buffer pool
        try{
            hashTable->lookup(file,pageNo,frameNo);
            bufDescTable[frameNo].refbit=true;
            bufDescTable[frameNo].pinCnt++;
            page = &bufPool[frameNo];
        }
        catch (const HashNotFoundException& e){
            allocBuf(frameNo);
            //read page to the new allocated frame
            bufPool[frameNo] = file->readPage(pageNo);
            bufStats.diskreads++;

            //update hashtable
            hashTable->insert(file,pageNo,frameNo);

            //reset the description table for this frame
            bufDescTable[frameNo].Set(file,pageNo);
            page = &bufPool[frameNo];
        }
        return;
    }

//do nothing if not found?
    void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
    {
        //look for the place of the page in the buffer pool
        //throw an exception if page not found in framepool and do nothing
        try {
            FrameId frameNo = 0;
            hashTable->lookup(file,pageNo,frameNo);

            //set the dirty bit if the passed in dirty is true
            if(dirty == true){
                bufDescTable[frameNo].dirty = true;
            }

            //if pincount is already 0, then throw an exception and no need to decrease pincnt
            if(bufDescTable[frameNo].pinCnt == 0){
                throw PageNotPinnedException(file->filename(),pageNo,frameNo);
            }
            else{
                bufDescTable[frameNo].pinCnt--;
            }
            return;
        }
        catch (const HashNotFoundException& e){
            return;
        }


    }

    void BufMgr::flushFile(const File* file)
    {
        FrameId i;
        BufDesc* tmpBufDesc;
        for(i=0; i < numBufs; i++){
            tmpBufDesc = &bufDescTable[i];
            if(tmpBufDesc->file == file) {
                if (tmpBufDesc->valid == false) {
                    throw BadBufferException(tmpBufDesc->frameNo, tmpBufDesc->dirty, tmpBufDesc->valid,
                                             tmpBufDesc->refbit);
                } else {
                    if (tmpBufDesc->pinCnt > 0) {
                        throw PagePinnedException(file->filename(), tmpBufDesc->pageNo, tmpBufDesc->frameNo);
                    }
                    if (tmpBufDesc->dirty == true) {
                        //Page newPage = tmpBufDesc->file->allocatePage();
                        tmpBufDesc->file->writePage(bufPool[i]);
                        tmpBufDesc->dirty = false;
                    }
                    hashTable->remove(file, tmpBufDesc->pageNo);
                    tmpBufDesc->Clear();
                }
            }
        }
        return;
    }

    void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
    {
        Page newPage = file->allocatePage();
        FrameId frameNo = 0;
        allocBuf(frameNo);
        bufPool[frameNo] = newPage;
        pageNo = newPage.page_number();
        page = &bufPool[frameNo];

        //insert the entry into hashtable
        hashTable->insert(file,pageNo,frameNo);

        //update the frame desciption table
        bufDescTable[frameNo].Set(file,pageNo);

        return;
    }

    void BufMgr::disposePage(File* file, const PageId PageNo)
    {
        try {
            //check if it is in the frame pool, if not, exit through the exception
            FrameId frameNo = 0;
            hashTable->lookup(file,PageNo,frameNo);

            //clear the page from frame if found
            bufDescTable[frameNo].Clear();

            //delete the entry in the hashtable of the found frame
            hashTable->remove(file,PageNo);

            //delete page from its file
            file->deletePage(PageNo);
            return;
        } catch (const HashNotFoundException& e){return;}




    }

    void BufMgr::printSelf(void)
    {
        BufDesc* tmpbuf;
        int validFrames = 0;

        for (std::uint32_t i = 0; i < numBufs; i++)
        {
            tmpbuf = &(bufDescTable[i]);
            std::cout << "FrameNo:" << i << " ";
            tmpbuf->Print();

            if (tmpbuf->valid == true)
                validFrames++;
        }

        std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
    }

}
