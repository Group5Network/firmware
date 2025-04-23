#include "ReliableRouter.h"
#include "Default.h"
#include "MeshTypes.h"
#include "configuration.h"
#include "mesh-pb-constants.h"
#include "modules/NodeInfoModule.h"
#include "modules/RoutingModule.h"

// ReliableRouter::ReliableRouter() {}

/**
 * If the message is want_ack, then add it to a list of packets to retransmit.
 * If we run out of retransmissions, send a nak packet towards the original client to indicate failure.
 */
ErrorCode ReliableRouter::send(meshtastic_MeshPacket *p)
{
    // Put how far we think we are from the destination into the packet header,
    // only if not a broadcast packet
    p->perceived_distance = (isBroadcast(p->to) || distance.find(p->to) == distance.end()) ? 0 : distance.find(p->to)->second;
    // Put our last byte
    p->current_hop = nodeDB->getLastByteOfNodeNum(getNodeNum());

    if (p->want_ack) {
        // If someone asks for acks on broadcast, we need the hop limit to be at least one, so that first node that receives our
        // message will rebroadcast.  But asking for hop_limit 0 in that context means the client app has no preference on hop
        // counts and we want this message to get through the whole mesh, so use the default.
        if (p->hop_limit == 0) {
            p->hop_limit = Default::getConfiguredOrDefaultHopLimit(config.lora.hop_limit);
        }

        auto copy = packetPool.allocCopy(*p);
        startRetransmission(copy);
    }

    /* If we have pending retransmissions, add the airtime of this packet to it, because during that time we cannot receive an
       (implicit) ACK. Otherwise, we might retransmit too early.
     */
    for (auto i = pending.begin(); i != pending.end(); i++) {
        if (i->first.id != p->id) {
            i->second.nextTxMsec += iface->getPacketTime(p);
        }
    }

    return FloodingRouter::send(p);
}

bool ReliableRouter::shouldFilterReceived(const meshtastic_MeshPacket *p)
{
    LOG_WARN("called ReliableRouter::shouldFilterReceived at this point"); // TODO remove maybe

    // Note: do not use getFrom() here, because we want to ignore messages sent from phone
    if (p->from == getNodeNum()) {
        printPacket("Rx someone rebroadcasting for us", p);
        // We are seeing someone rebroadcast one of our broadcast attempts.
        // If this is the first time we saw this, cancel any retransmissions we have queued up and generate an internal ack for
        // the original sending process.
        // This "optimization", does save lots of airtime. For DMs, you also get a real ACK back
        // from the intended recipient.
        auto key = GlobalPacketId(getFrom(p), p->id);
        auto old = findPendingPacket(key);
        if (old) {
            LOG_DEBUG("Generate implicit ack");
            // NOTE: we do NOT check p->wantAck here because p is the INCOMING rebroadcast and that packet is not expected to be
            // marked as wantAck
            sendAckNak(meshtastic_Routing_Error_NONE, getFrom(p), p->id, old->packet->channel);

            stopRetransmission(key);
        } else {
            LOG_DEBUG("Didn't find pending packet");
        }
    }

    /* At this point we have already deleted the pending retransmission if this packet was an (implicit) ACK to it.
       Now for all other pending retransmissions, we have to add the airtime of this received packet to the retransmission timer,
       because while receiving this packet, we could not have received an (implicit) ACK for it.
       If we don't add this, we will likely retransmit too early.
    */
    for (auto i = pending.begin(); i != pending.end(); i++) {
        i->second.nextTxMsec += iface->getPacketTime(p);
    }

    return FloodingRouter::shouldFilterReceived(p);
}

/**
 * If we receive a want_ack packet (do not check for wasSeenRecently), send back an ack (this might generate multiple ack sends in
 * case the our first ack gets lost)
 *
 * If we receive an ack packet (do check wasSeenRecently), clear out any retransmissions and
 * forward the ack to the application layer.
 *
 * If we receive a nak packet (do check wasSeenRecently), clear out any retransmissions
 * and forward the nak to the application layer.
 *
 * Otherwise, let superclass handle it.
 */
void ReliableRouter::sniffReceived(const meshtastic_MeshPacket *p, const meshtastic_Routing *c)
{
    // DEBUGGING
    LOG_WARN("distances map contents:");
    for (auto it = distance.begin(); it != distance.end(); it++) {
        LOG_WARN("node %08x: %d distance", it->first, it->second);
    }

    // whether the distance to the node was just set for the first time,
    // true only in the case of a packet not destined for us where
    // senders_distance > 0 and our_distance = 0.
    // to ensure that in this case the packet is transmitted onwards
    bool distance_was_just_set = false;

    // find the sender of this packet
    // if we find it, update our distance to it to 1
    NodeNum sender = nodeDB->findMatchingNodeNum(p->current_hop);
    LOG_WARN("current sender lookup: %02x -> %08x", p->current_hop, sender);

    if (sender != 0) {
        distance.erase(sender);
        distance.insert(std::make_pair(sender, 1));
        LOG_WARN("Update distance to direct neighbour %08x to 1", sender);
    }
    // if the packet is to us, update our distance to the original sender to
    // hop_start - hop_limit + 1
    // note the packet may have taken multiple paths to get here, and
    // be received in the future after travelling more hops, so we only update
    // if it is less than the current distance: min(current distance, hop_start - hop_limit + 1)
    if (isToUs(p)) {
        uint8_t dist = p->hop_start - p->hop_limit + 1;
        auto from_node = distance.find(p->from);

        if (from_node == distance.end()) {
            // not found a distance to original sender, set to hop_start - hop_limit + 1
            LOG_WARN("Packet is to us, no prior distance to sender, update distance to %08x to %d", p->from, dist);
            distance.insert(std::make_pair(p->from, dist));
        } else {
            // found a previous distance to original sender, change distance if smaller
            auto prev_dist = from_node->second;
            distance.erase(p->from);
            distance.insert(std::make_pair(p->from, min(dist, prev_dist)));
            if (dist < prev_dist) {
                LOG_WARN("Packet is to us, distance %d < previous dist %d, update distance to %08x to %d",
                    dist, prev_dist, p->from, dist
                );
            } else {
                LOG_WARN("Packet is to us, distance %d >= previous dist %d, distance to %08x remains %d",
                    dist, prev_dist, p->from, prev_dist
                );
            }
        }

    } else if (!isBroadcast(p->to)) {
        // update our distance to the destination here
        auto senders_distance = p->perceived_distance;
        auto our_distance = (distance.find(p->to) == distance.end()) ? 0 : distance.find(p->to)->second;

        if (senders_distance == 0) {
            // sender doesn't tell us any information, we do not update our distance
        }

        else if (senders_distance > 0 && our_distance == 0) {
            // sender claims distance of n and we do not know our distance, set our distance to n + 1 (worst case),
            // also set distance_was_just_set to true
            distance_was_just_set = true;
            LOG_WARN("update distance to %08x to sender dist + 1 (%d + 1 = %d)", p->to, senders_distance, senders_distance + 1);
            assert(distance.find(p->to) == distance.end());
            distance.insert(std::make_pair(p->to, senders_distance + 1));
        }

        else if (senders_distance > 0 && our_distance > 0) {
            LOG_WARN("sender distance: %d, our distance: %d", senders_distance, our_distance);
            // if the sender has a distance that is more than 1 less, we set our distance to the senders + 1
            if (senders_distance + 1 < our_distance) {
                LOG_WARN("set our distance %08x to sender dist + 1 (%d + 1 = %d)", p->to, senders_distance, senders_distance + 1);
                distance.erase(p->to);
                distance.insert(std::make_pair(p->to, senders_distance + 1));
            }
        }
    }

    if (isToUs(p)) { // ignore ack/nak/want_ack packets that are not address to us (we only handle 0 hop reliability)
        if (p->want_ack) {
            if (MeshModule::currentReply) {
                LOG_DEBUG("Another module replied to this message, no need for 2nd ack");
            } else if (p->which_payload_variant == meshtastic_MeshPacket_decoded_tag) {
                // A response may be set to want_ack for retransmissions, but we don't need to ACK a response if it received an
                // implicit ACK already. If we received it directly, only ACK with a hop limit of 0
                if (!p->decoded.request_id)
                    sendAckNak(meshtastic_Routing_Error_NONE, getFrom(p), p->id, p->channel,
                               routingModule->getHopLimitForResponse(p->hop_start, p->hop_limit));
                else if (p->hop_start > 0 && p->hop_start == p->hop_limit)
                    sendAckNak(meshtastic_Routing_Error_NONE, getFrom(p), p->id, p->channel, 0);
            } else if (p->which_payload_variant == meshtastic_MeshPacket_encrypted_tag && p->channel == 0 &&
                       (nodeDB->getMeshNode(p->from) == nullptr || nodeDB->getMeshNode(p->from)->user.public_key.size == 0)) {
                LOG_INFO("PKI packet from unknown node, send PKI_UNKNOWN_PUBKEY");
                sendAckNak(meshtastic_Routing_Error_PKI_UNKNOWN_PUBKEY, getFrom(p), p->id, channels.getPrimaryIndex(),
                           routingModule->getHopLimitForResponse(p->hop_start, p->hop_limit));
            } else {
                // Send a 'NO_CHANNEL' error on the primary channel if want_ack packet destined for us cannot be decoded
                sendAckNak(meshtastic_Routing_Error_NO_CHANNEL, getFrom(p), p->id, channels.getPrimaryIndex(),
                           routingModule->getHopLimitForResponse(p->hop_start, p->hop_limit));
            }
        }
        if (p->which_payload_variant == meshtastic_MeshPacket_decoded_tag && c &&
            c->error_reason == meshtastic_Routing_Error_PKI_UNKNOWN_PUBKEY) {
            if (owner.public_key.size == 32) {
                LOG_INFO("PKI decrypt failure, send a NodeInfo");
                nodeInfoModule->sendOurNodeInfo(p->from, false, p->channel, true);
            }
        }
        // We consider an ack to be either a !routing packet with a request ID or a routing packet with !error
        PacketId ackId = ((c && c->error_reason == meshtastic_Routing_Error_NONE) || !c) ? p->decoded.request_id : 0;

        // A nak is a routing packt that has an error code
        PacketId nakId = (c && c->error_reason != meshtastic_Routing_Error_NONE) ? p->decoded.request_id : 0;

        // We intentionally don't check wasSeenRecently, because it is harmless to delete non existent retransmission records
        if (ackId || nakId) {
            LOG_DEBUG("Received a %s for 0x%x, stopping retransmissions", ackId ? "ACK" : "NAK", ackId);
            if (ackId) {
                stopRetransmission(p->to, ackId);
            } else {
                stopRetransmission(p->to, nakId);
            }
        }
    }

    bool isAckorReply = (p->which_payload_variant == meshtastic_MeshPacket_decoded_tag) && (p->decoded.request_id != 0);
    if (isAckorReply && !isToUs(p) && !isBroadcast(p->to)) {
        // do not flood direct message that is ACKed or replied to
        LOG_DEBUG("Rxd an ACK/reply not for me, cancel rebroadcast");
        Router::cancelSending(p->to, p->decoded.request_id); // cancel rebroadcast for this DM
    }

    ReliableRouter::perhapsRebroadcast(p, distance_was_just_set);

    // handle the packet as normal
    FloodingRouter::sniffReceived(p, c);
}

bool ReliableRouter::perhapsRebroadcast(const meshtastic_MeshPacket *p, const bool distance_was_just_set) {
    LOG_WARN("called ReliableRouter::shouldFilterReceived at this point");

    if (isBroadcast(p->to)) {
        LOG_WARN("is broadcast packet, skip distance checks (flood)");
        return FloodingRouter::perhapsRebroadcast(p);
    }

    auto senders_distance = p->perceived_distance;
    auto our_distance = (distance.find(p->to) == distance.end()) ? 0 : distance.find(p->to)->second;

    LOG_WARN("senders distance: %d, our distance: %d, just set: %s",
        senders_distance, our_distance, distance_was_just_set ? "true" : "false");

    if (our_distance == 0) {
        LOG_WARN("our distance unknown, fallthrough (flood)");
    } else {
        assert(our_distance > 0);
        if (senders_distance == 0) {
            LOG_WARN("sender distance unknown, fallthrough (flood)");
        } else {
            assert(senders_distance > 0);
            // if our distance to the destination was just set for the first time,
            // i.e our distance was unknown and the senders distance was n,
            // we have set our distance to be n + 1, but we must make sure in this
            // specific case we do retransmit the message
            if (distance_was_just_set) {
                // fallthrough
                LOG_WARN("our distance was just set, fallthrough (flood)");
            } else if (our_distance >= senders_distance) {
                LOG_WARN("our distance is known to be greater than or equal to sender, do NOT retransmit!");
                return true;
            } else {
                LOG_WARN("our distance is lesser, fallthrough (flood)");
            }
        }
    }

    return FloodingRouter::perhapsRebroadcast(p);
}

#define NUM_RETRANSMISSIONS 3

PendingPacket::PendingPacket(meshtastic_MeshPacket *p)
{
    packet = p;
    numRetransmissions = NUM_RETRANSMISSIONS - 1; // We subtract one, because we assume the user just did the first send
}

PendingPacket *ReliableRouter::findPendingPacket(GlobalPacketId key)
{
    auto old = pending.find(key); // If we have an old record, someone messed up because id got reused
    if (old != pending.end()) {
        return &old->second;
    } else
        return NULL;
}
/**
 * Stop any retransmissions we are doing of the specified node/packet ID pair
 */
bool ReliableRouter::stopRetransmission(NodeNum from, PacketId id)
{
    auto key = GlobalPacketId(from, id);
    return stopRetransmission(key);
}

bool ReliableRouter::stopRetransmission(GlobalPacketId key)
{
    auto old = findPendingPacket(key);
    if (old) {
        auto p = old->packet;
        /* Only when we already transmitted a packet via LoRa, we will cancel the packet in the Tx queue
          to avoid canceling a transmission if it was ACKed super fast via MQTT */
        if (old->numRetransmissions < NUM_RETRANSMISSIONS - 1) {
            // remove the 'original' (identified by originator and packet->id) from the txqueue and free it
            cancelSending(getFrom(p), p->id);
        }
        // now free the pooled copy for retransmission too
        packetPool.release(p);
        auto numErased = pending.erase(key);
        assert(numErased == 1);
        return true;
    } else
        return false;
}

/**
 * Add p to the list of packets to retransmit occasionally.  We will free it once we stop retransmitting.
 */
PendingPacket *ReliableRouter::startRetransmission(meshtastic_MeshPacket *p)
{
    auto id = GlobalPacketId(p);
    auto rec = PendingPacket(p);

    stopRetransmission(getFrom(p), p->id);

    setNextTx(&rec);
    pending[id] = rec;

    return &pending[id];
}

/**
 * Do any retransmissions that are scheduled (FIXME - for the time being called from loop)
 */
int32_t ReliableRouter::doRetransmissions()
{
    uint32_t now = millis();
    int32_t d = INT32_MAX;

    // FIXME, we should use a better datastructure rather than walking through this map.
    // for(auto el: pending) {
    for (auto it = pending.begin(), nextIt = it; it != pending.end(); it = nextIt) {
        ++nextIt; // we use this odd pattern because we might be deleting it...
        auto &p = it->second;

        bool stillValid = true; // assume we'll keep this record around

        // FIXME, handle 51 day rollover here!!!
        if (p.nextTxMsec <= now) {
            if (p.numRetransmissions == 0) {
                // Out of retransmissions:
                // TODO Set all our distances to nodes with a distance of 1 (immediate neighbours) to unknown.

                LOG_DEBUG("Reliable send failed, return a nak for fr=0x%x,to=0x%x,id=0x%x", p.packet->from, p.packet->to,
                          p.packet->id);
                sendAckNak(meshtastic_Routing_Error_MAX_RETRANSMIT, getFrom(p.packet), p.packet->id, p.packet->channel);
                // Note: we don't stop retransmission here, instead the Nak packet gets processed in sniffReceived
                stopRetransmission(it->first);
                stillValid = false; // just deleted it
            } else {
                LOG_DEBUG("Send reliable retransmission fr=0x%x,to=0x%x,id=0x%x, tries left=%d", p.packet->from, p.packet->to,
                          p.packet->id, p.numRetransmissions);

                // Note: we call the superclass version because we don't want to have our version of send() add a new
                // retransmission record
                FloodingRouter::send(packetPool.allocCopy(*p.packet));

                // Queue again
                --p.numRetransmissions;
                setNextTx(&p);
            }
        }

        if (stillValid) {
            // Update our desired sleep delay
            int32_t t = p.nextTxMsec - now;

            d = min(t, d);
        }
    }

    return d;
}

void ReliableRouter::setNextTx(PendingPacket *pending)
{
    assert(iface);
    auto d = iface->getRetransmissionMsec(pending->packet);
    pending->nextTxMsec = millis() + d;
    LOG_DEBUG("Set next retransmission in %u msecs: ", d);
    printPacket("", pending->packet);
    setReceivedMessage(); // Run ASAP, so we can figure out our correct sleep time
}