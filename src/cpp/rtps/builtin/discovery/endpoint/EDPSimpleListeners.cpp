// Copyright 2016 Proyectos y Sistemas de Mantenimiento SL (eProsima).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @file EDPSimpleListener.cpp
 *
 */

#include "EDPSimpleListeners.h"

#include <fastdds/dds/log/Log.hpp>

#include <fastdds/rtps/builtin/data/ParticipantProxyData.h>
#include <fastdds/rtps/builtin/data/ReaderProxyData.h>
#include <fastdds/rtps/builtin/data/WriterProxyData.h>
#include <fastdds/rtps/builtin/discovery/endpoint/EDPSimple.h>
#include <fastdds/rtps/builtin/discovery/endpoint/EDPServer.h>
#include <fastdds/rtps/builtin/discovery/participant/PDPSimple.h>

#include <fastdds/rtps/common/InstanceHandle.h>
#include <fastdds/rtps/history/ReaderHistory.h>
#include <fastdds/rtps/history/WriterHistory.h>
#include <fastdds/rtps/network/NetworkFactory.h>
#include <fastdds/rtps/reader/StatefulReader.h>
#include <fastdds/rtps/writer/StatefulWriter.h>

#include <fastdds/core/policy/ParameterList.hpp>
#include <fastrtps_deprecated/participant/ParticipantImpl.h>

#include <mutex>

using ParameterList = eprosima::fastdds::dds::ParameterList;

// Release reader lock to avoid ABBA lock. PDP mutex should always be first.
// Keep change information on local variables to check consistency later
#define PREVENT_PDP_DEADLOCK(reader, change, pdp)                         \
    GUID_t writer_guid = (change)->writerGUID;                            \
    SequenceNumber_t seq_num = (change)->sequenceNumber;                  \
    (reader)->getMutex().unlock();                                        \
    std::unique_lock<std::recursive_mutex> lock(*((pdp)->getMutex()));    \
    (reader)->getMutex().lock();                                          \
                                                                          \
    if ((ALIVE != (change)->kind) ||                                     \
            (seq_num != (change)->sequenceNumber) ||                         \
            (writer_guid != (change)->writerGUID))                          \
    {                                                                     \
        return;                                                           \
    }                                                                     \
    (void)seq_num

namespace eprosima {
namespace fastrtps {
namespace rtps {

void EDPBasePUBListener::add_writer_from_change(
        RTPSReader* reader,
        ReaderHistory* reader_history,
        CacheChange_t* change,
        EDP* edp,
        bool release_change /*=true*/)
{
    //LOAD INFORMATION IN DESTINATION WRITER PROXY DATA
    const NetworkFactory& network = edp->mp_RTPSParticipant->network_factory();
    CDRMessage_t tempMsg(change->serializedPayload);
    if (temp_writer_data_.readFromCDRMessage(&tempMsg, network,
            edp->mp_RTPSParticipant->has_shm_transport()))
    {
        change->instanceHandle = temp_writer_data_.key();
        if (temp_writer_data_.guid().guidPrefix == edp->mp_RTPSParticipant->getGuid().guidPrefix
                && !ongoingDeserialization(edp))
        {
            logInfo(RTPS_EDP, "Message from own RTPSParticipant, ignoring");
            return;
        }

        std::vector<std::string> names= edp->mp_PDP->getRTPSParticipant()->getParticipantNames();
        // std::vector<std::string> names= this->sedp_->mp_PDP->getRTPSParticipant()->getParticipantNames();

        std::string this_name = names.front();
        std::string topic_name = static_cast<std::string>(temp_writer_data_.topicName());

        std::cout<<"PUB MESSAGE Activating: " << temp_writer_data_.guid().entityId << " in topic " << topic_name << std::endl;
        std::cout<<"PARTICIPANT PROXY DATA: "<< this_name<<std::endl;

        std::set<std::pair<std::string, std::string>> allowlist;

        // this is the PUBLISHER listener
        // create a list of pairs of nodes and topics they subscribe to
        // e.g. lyon subscribes to amazon, so has to accept publishers related to amazon
        // note that the ROS topic names are prepended with "rt/" by the DDS
        allowlist.insert(std::make_pair("lyon", "rt/amazon"));
        allowlist.insert(std::make_pair("hamburg", "rt/nile"));
        allowlist.insert(std::make_pair("hamburg", "rt/tigris"));
        allowlist.insert(std::make_pair("hamburg", "rt/ganges"));
        allowlist.insert(std::make_pair("hamburg", "rt/danube"));
        allowlist.insert(std::make_pair("osaka", "rt/parana"));
        allowlist.insert(std::make_pair("mandalay", "rt/salween"));
        allowlist.insert(std::make_pair("mandalay", "rt/danube"));
        allowlist.insert(std::make_pair("ponce", "rt/missouri"));
        allowlist.insert(std::make_pair("ponce", "rt/danube"));
        allowlist.insert(std::make_pair("ponce", "rt/volga"));
        allowlist.insert(std::make_pair("barcelona", "rt/mekong"));
        allowlist.insert(std::make_pair("georgetown", "rt/lena"));
        allowlist.insert(std::make_pair("geneva", "rt/congo"));
        allowlist.insert(std::make_pair("geneva", "rt/danube"));
        allowlist.insert(std::make_pair("geneva", "rt/parana"));
        allowlist.insert(std::make_pair("arequipa", "rt/arkansas"));

        std::set<std::pair<std::string, std::string>> t_list;

        // augment the list with additional topics for intraprocess
        // NOTE: if you only have intraprocess communication,
        // you could directly use only the "/_intra" topics
        for (auto p : allowlist){
            t_list.insert(std::make_pair(p.first, p.second));
            std::string intra_name = std::string(p.second) + std::string("/_intra");
            t_list.insert(std::make_pair(p.first, intra_name));
        }

        allowlist = t_list;

        if (allowlist.find(std::make_pair(this_name, topic_name)) == allowlist.end()){
            std::cout<<"DISCARDED topic"<<std::endl;
            return;
        }

        std::cout<<"APPROVED topic"<<std::endl;

        //LOAD INFORMATION IN DESTINATION WRITER PROXY DATA
        auto copy_data_fun = [this, &network](
            WriterProxyData* data,
            bool updating,
            const ParticipantProxyData& participant_data)
                {
                    if (!temp_writer_data_.has_locators())
                    {
                        temp_writer_data_.set_remote_locators(participant_data.default_locators, network, true);
                    }

                    if (updating && !data->is_update_allowed(temp_writer_data_))
                    {
                        logWarning(RTPS_EDP,
                                "Received incompatible update for WriterQos. writer_guid = " << data->guid());
                    }
                    *data = temp_writer_data_;
                    return true;
                };

        GUID_t participant_guid;
        WriterProxyData* writer_data =
                edp->mp_PDP->addWriterProxyData(temp_writer_data_.guid(), participant_guid, copy_data_fun);

        //Removing change from history
        reader_history->remove_change(reader_history->find_change(change), release_change);

        // At this point we can release reader lock, cause change is not used
        reader->getMutex().unlock();
        if (writer_data != nullptr)
        {
            edp->pairing_writer_proxy_with_any_local_reader(participant_guid, writer_data);
        }
        else //NOT ADDED BECAUSE IT WAS ALREADY THERE
        {
            logWarning(RTPS_EDP, "Received message from UNKNOWN RTPSParticipant, removing");
        }
        // Take again the reader lock.
        reader->getMutex().lock();
    }
}

void EDPSimplePUBListener::onNewCacheChangeAdded(
        RTPSReader* reader,
        const CacheChange_t* const change_in)
{
    CacheChange_t* change = (CacheChange_t*)change_in;
    //std::lock_guard<std::recursive_mutex> guard(*this->sedp_->publications_reader_.first->getMutex());
    logInfo(RTPS_EDP, "");
    if (!computeKey(change))
    {
        logWarning(RTPS_EDP, "Received change with no Key");
    }

    ReaderHistory* reader_history =
#if HAVE_SECURITY
            reader == sedp_->publications_secure_reader_.first ?
            sedp_->publications_secure_reader_.second :
#endif // if HAVE_SECURITY
            sedp_->publications_reader_.second;

    if (change->kind == ALIVE)
    {
        PREVENT_PDP_DEADLOCK(reader, change, sedp_->mp_PDP);

        // Note: change is removed from history inside this method.
        add_writer_from_change(reader, reader_history, change, sedp_);
    }
    else
    {
        //REMOVE WRITER FROM OUR READERS:
        logInfo(RTPS_EDP, "Disposed Remote Writer, removing...");
        GUID_t writer_guid = iHandle2GUID(change->instanceHandle);
        //Removing change from history
        reader_history->remove_change(change);
        reader->getMutex().unlock();
        this->sedp_->mp_PDP->removeWriterProxyData(writer_guid);
        reader->getMutex().lock();
    }
}

bool EDPListener::computeKey(
        CacheChange_t* change)
{
    return ParameterList::readInstanceHandleFromCDRMsg(change, fastdds::dds::PID_ENDPOINT_GUID);
}

bool EDPListener::ongoingDeserialization(
        EDP* edp)
{
    EDPServer* pServer = dynamic_cast<EDPServer*>(edp);

    if (pServer)
    {
        return pServer->ongoingDeserialization();
    }

    return false;
}

void EDPBaseSUBListener::add_reader_from_change(
        RTPSReader* reader,
        ReaderHistory* reader_history,
        CacheChange_t* change,
        EDP* edp,
        bool release_change /*=true*/)
{
    //LOAD INFORMATION IN TEMPORAL WRITER PROXY DATA
    const NetworkFactory& network = edp->mp_RTPSParticipant->network_factory();
    CDRMessage_t tempMsg(change->serializedPayload);
    if (temp_reader_data_.readFromCDRMessage(&tempMsg, network,
            edp->mp_RTPSParticipant->has_shm_transport()))
    {
        change->instanceHandle = temp_reader_data_.key();
        if (temp_reader_data_.guid().guidPrefix == edp->mp_RTPSParticipant->getGuid().guidPrefix
                && !ongoingDeserialization(edp))
        {
            logInfo(RTPS_EDP, "From own RTPSParticipant, ignoring");
            return;
        }

        // std::vector<std::string> names= this->sedp_->mp_PDP->getRTPSParticipant()->getParticipantNames();
        std::vector<std::string> names = edp->mp_RTPSParticipant->getParticipantNames();

        std::string this_name = names.front();
        std::string topic_name = static_cast<std::string>(temp_reader_data_.topicName());

        std::cout<<"SUB MESSAGE Activating: " << temp_reader_data_.guid().entityId << " in topic " << topic_name << std::endl;
        std::cout<<"PARTICIPANT PROXY DATA: "<< this_name<<std::endl;

        std::set<std::pair<std::string, std::string>> allowlist;

        // this is the SUBSCRIBER listener
        // create a list of pairs of nodes and topics they publish to
        // e.g. montreal publishes to amazon, so has to accept subscribers related to amazon
        // note that the ROS topic names are prepended with "rt/" by the DDS
        allowlist.insert(std::make_pair("montreal", "rt/amazon"));
        allowlist.insert(std::make_pair("montreal", "rt/nile"));
        allowlist.insert(std::make_pair("montreal", "rt/ganges"));
        allowlist.insert(std::make_pair("montreal", "rt/danube"));
        allowlist.insert(std::make_pair("lyon", "rt/tigris"));
        allowlist.insert(std::make_pair("hamburg", "rt/parana"));
        allowlist.insert(std::make_pair("osaka", "rt/salween"));
        allowlist.insert(std::make_pair("mandalay", "rt/missouri"));
        allowlist.insert(std::make_pair("ponce", "rt/mekong"));
        allowlist.insert(std::make_pair("ponce", "rt/congo"));
        allowlist.insert(std::make_pair("barcelona", "rt/lena"));
        allowlist.insert(std::make_pair("georgetown", "rt/volga"));
        allowlist.insert(std::make_pair("geneva", "rt/arkansas"));

        std::set<std::pair<std::string, std::string>> t_list;

        // augment the list with additional topics for intraprocess
        // NOTE: if you only have intraprocess communication,
        // you could directly use only the "/_intra" topics
        for (auto p : allowlist){
            t_list.insert(std::make_pair(p.first, p.second));
            std::string intra_name = std::string(p.second) + std::string("/_intra");
            t_list.insert(std::make_pair(p.first, intra_name));
        }

        allowlist = t_list;

        if (allowlist.find(std::make_pair(this_name, topic_name)) == allowlist.end()){
            std::cout<<"DISCARDED topic"<<std::endl;
            return;
        }

        std::cout<<"APPROVED topic"<<std::endl;

        auto copy_data_fun = [this, &network](
            ReaderProxyData* data,
            bool updating,
            const ParticipantProxyData& participant_data)
                {
                    if (!temp_reader_data_.has_locators())
                    {
                        temp_reader_data_.set_remote_locators(participant_data.default_locators, network, true);
                    }

                    if (updating && !data->is_update_allowed(temp_reader_data_))
                    {
                        logWarning(RTPS_EDP,
                                "Received incompatible update for ReaderQos. reader_guid = " << data->guid());
                    }
                    *data = temp_reader_data_;
                    return true;
                };

        //LOOK IF IS AN UPDATED INFORMATION
        GUID_t participant_guid;
        ReaderProxyData* reader_data =
                edp->mp_PDP->addReaderProxyData(temp_reader_data_.guid(), participant_guid, copy_data_fun);

        // Remove change from history.
        reader_history->remove_change(reader_history->find_change(change), release_change);

        // At this point we can release reader lock, cause change is not used
        reader->getMutex().unlock();

        if (reader_data != nullptr) //ADDED NEW DATA
        {
            edp->pairing_reader_proxy_with_any_local_writer(participant_guid, reader_data);

        }
        else
        {
            logWarning(RTPS_EDP, "From UNKNOWN RTPSParticipant, removing");
        }

        // Take again the reader lock.
        reader->getMutex().lock();
    }
}

void EDPSimpleSUBListener::onNewCacheChangeAdded(
        RTPSReader* reader,
        const CacheChange_t* const change_in)
{
    CacheChange_t* change = (CacheChange_t*)change_in;
    //std::lock_guard<std::recursive_mutex> guard(*this->sedp_->subscriptions_reader_.first->getMutex());
    logInfo(RTPS_EDP, "");
    if (!computeKey(change))
    {
        logWarning(RTPS_EDP, "Received change with no Key");
    }

    ReaderHistory* reader_history =
#if HAVE_SECURITY
            reader == sedp_->subscriptions_secure_reader_.first ?
            sedp_->subscriptions_secure_reader_.second :
#endif // if HAVE_SECURITY
            sedp_->subscriptions_reader_.second;

    if (change->kind == ALIVE)
    {
        PREVENT_PDP_DEADLOCK(reader, change, sedp_->mp_PDP);

        // Note: change is removed from history inside this method.
        add_reader_from_change(reader, reader_history, change, sedp_);
    }
    else
    {
        //REMOVE WRITER FROM OUR READERS:
        logInfo(RTPS_EDP, "Disposed Remote Reader, removing...");

        GUID_t reader_guid = iHandle2GUID(change->instanceHandle);
        //Removing change from history
        reader_history->remove_change(change);
        reader->getMutex().unlock();
        this->sedp_->mp_PDP->removeReaderProxyData(reader_guid);
        reader->getMutex().lock();
    }
}

void EDPSimplePUBListener::onWriterChangeReceivedByAll(
        RTPSWriter* writer,
        CacheChange_t* change)
{
    (void)writer;

    if (ChangeKind_t::NOT_ALIVE_DISPOSED_UNREGISTERED == change->kind)
    {
        WriterHistory* writer_history =
#if HAVE_SECURITY
                writer == sedp_->publications_secure_writer_.first ?
                sedp_->publications_secure_writer_.second :
#endif // if HAVE_SECURITY
                sedp_->publications_writer_.second;

        writer_history->remove_change(change);
    }
}

void EDPSimpleSUBListener::onWriterChangeReceivedByAll(
        RTPSWriter* writer,
        CacheChange_t* change)
{
    (void)writer;

    if (ChangeKind_t::NOT_ALIVE_DISPOSED_UNREGISTERED == change->kind)
    {
        WriterHistory* writer_history =
#if HAVE_SECURITY
                writer == sedp_->subscriptions_secure_writer_.first ?
                sedp_->subscriptions_secure_writer_.second :
#endif // if HAVE_SECURITY
                sedp_->subscriptions_writer_.second;

        writer_history->remove_change(change);
    }

}

} /* namespace rtps */
} /* namespace fastrtps */
} /* namespace eprosima */
