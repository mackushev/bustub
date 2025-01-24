//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

#include <algorithm>

namespace bustub {

// one is older than two
static bool older(const LRUKAge& one, const LRUKAge& two) 
{
    if ( one.kAccess_.has_value() ) {
        if ( two.kAccess_.has_value() ) {
            BUSTUB_ASSERT( one.kAccess_ != two.kAccess_, "Invariant kAccess failed" );
            // older is one that created earlier
            return one.kAccess_ < two.kAccess_;
        }
        // inf in two, !inf in one. two is older.   
        return false;
    } else if( two.kAccess_.has_value() ) {
        // inf in one, !inf at two. one is older  
        return true;
    }

    // inf here, inf there, mru
    BUSTUB_ASSERT( one.lAccess_ != two.lAccess_, "Invariant lAccess failed" );
    return one.lAccess_ < two.lAccess_;
}

// this < other means. 
bool LRUKAge::operator<( const LRUKAge& other) const
{
    return older(other, *this);
}

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t>
{ 
    // check if we have smth to evict
    if ( evictable.empty() ) {
        return std::nullopt;
    }
    
    // extract pretendent from evictable queue
    std::pop_heap( evictable.begin(), evictable.end() );
    const auto to_evict = evictable.back();
    evictable.pop_back();
    
    // extract from node store
    node_store_.erase( to_evict.fid_ );
    return to_evict.fid_;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type)
{
    // 0! increase time  
    const int accessTime = current_timestamp_.fetch_add( 1 ); 

    // try to add new frame  
    auto [i, iniserted] = node_store_.emplace( frame_id, LRUKNode(frame_id, accessTime) );
    
    // if already existed - add access history (and check if its not evictable)
    if ( not iniserted ) {
        
        // register new access
        i->second.history_.push_front( accessTime );

        if( i->second.hCount_ >= k_ ) {
            i->second.history_.pop_back();
        } else {
            i->second.hCount_ += 1;
        }

        //  if frame is in evictable list - need to be repositioned 
        if ( i->second.is_evictable_ ) {
            removeEvictable( i->second.fid_ );
            addEvictable( i->second );
        }   
    }

}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable( frame_id_t frame_id, bool set_evictable)
{
    // find what to check  
    auto pos = node_store_.find(frame_id);
    if( pos == node_store_.end()) {
        return;
    }

    //  check if state is not changed
    if ( pos->second.is_evictable_ == set_evictable ) {
        return;
    }

    if ( set_evictable ) {
        // need to add to evictable heap. 
        pos->second.is_evictable_ = true;
        addEvictable( pos->second );
    } else {
        // need to remove from evictable heap.
        pos->second.is_evictable_ = false;
        removeEvictable( pos->second.fid_ );
    }

}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id)
{
    auto pos = node_store_.find(frame_id);
    BUSTUB_ASSERT( pos != node_store_.end(), "frame to evict is not registered");
    if ( pos->second.is_evictable_ ) {
        removeEvictable( pos->second.fid_ );
    }
    node_store_.erase(pos->first);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t 
{
    return evictable.size();    
}

void LRUKReplacer::removeEvictable( frame_id_t fid )
{
    auto at = std::find_if( evictable.begin(), evictable.end(), [&fid](const auto& i) { return i.fid_ == fid; });
    *at = evictable.back();
    evictable.pop_back();
    if ( !evictable.empty() ) {
        std::make_heap( evictable.begin(), evictable.end() );
    }
}

void LRUKReplacer::addEvictable( const LRUKNode& node )
{
    BUSTUB_ASSERT( node.is_evictable_, "no message");

    LRUKAge to{ node.fid_, node.history_.front(),( node.hCount_ < k_) ? std::nullopt : std::optional<size_t>(node.history_.back()) };
    evictable.push_back( to );
    std::push_heap( evictable.begin(), evictable.end() );
    BUSTUB_ASSERT( std::is_heap(evictable.begin(), evictable.end()), "heap check" );
}

}  // namespace bustub
