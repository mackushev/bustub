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


// this < other means. 
bool LRUKAge::operator<( const LRUKAge& other) const
{
    if ( kAccess_.has_value() ) {
        if ( other.kAccess_.has_value() ) {
            BUSTUB_ASSERT( kAccess_ != other.kAccess_, "Invariant kAccess failed" );
            // less is one that created later
            return kAccess_ > other.kAccess_;
        }
        // inf in other, !inf here. other is older.   
        return true;
    } else if( other.kAccess_.has_value() ) {
        // inf here, !inf at other, here older and bigger  
        return false;
    }

    // inf here, inf there, mru
    BUSTUB_ASSERT( lAccess_ != other.lAccess_, "Invariant lAccess failed" );
    return lAccess_ > other.lAccess_;

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
    std::scoped_lock lock(latch_store_, latch_evictable_);

    // check if we have smth to evict
    if ( evictable_.empty() ) {
        return std::nullopt;
    }

    if ( update_eviction_.size() > 0 ) {
        std::for_each( evictable_.begin(), evictable_.end(), [&upd_set=update_eviction_](LRUKAge& a){ 
            auto update = upd_set.find(a.fid_);
            if ( update != upd_set.end() ) {
                a = update->second;
            }
        });
        update_eviction_.clear();
        std::make_heap( evictable_.begin(), evictable_.end() );
    }
    
    // extract pretendent from evictable queue
    std::pop_heap( evictable_.begin(), evictable_.end() );
    const auto to_evict = evictable_.back();
    evictable_.pop_back();
    
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

    std::lock_guard store_guard( latch_store_ );

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

        //  if frame is in evictable list - need to be repositioned on Eviction
        if ( i->second.is_evictable_ ) {
            on_updatedEvictable( i->second );
        }
    }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable_. This function also
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
    std::lock_guard lock(latch_store_);

    // find what to check  
    auto pos = node_store_.find(frame_id);
    // just ignore not accesses pages 
    if( pos == node_store_.end()) {
        return;
    }

    //  check if state is not changed ( behaviour from from test )
    if ( pos->second.is_evictable_ == set_evictable ) {
        return;
    }
    pos->second.is_evictable_ = set_evictable;
    pos->second.is_evictable_ ? addEvictable( pos->second ) : removeEvictable ( pos->second.fid_ );
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
    std::lock_guard giard( latch_store_ );

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
    std::lock_guard lock( latch_evictable_ );
    return evictable_.size();    
}

void LRUKReplacer::removeEvictable( frame_id_t fid )
{
    std::lock_guard lock( latch_evictable_ );

    update_eviction_.erase( fid );

    auto at = std::find_if( evictable_.begin(), evictable_.end(), [&fid](const auto& i) { return i.fid_ == fid; });
    *at = evictable_.back();
    evictable_.pop_back();
    if ( !evictable_.empty() ) {
        std::make_heap( evictable_.begin(), evictable_.end() );
    }
}

void LRUKReplacer::addEvictable( const LRUKNode& node )
{
    std::lock_guard lock( latch_evictable_ );

    BUSTUB_ASSERT( node.is_evictable_, "no message");
    evictable_.push_back( fromNode(node) );
    std::push_heap( evictable_.begin(), evictable_.end() );
    BUSTUB_ASSERT( std::is_heap(evictable_.begin(), evictable_.end()), "heap check" );
}

LRUKAge LRUKReplacer::fromNode( const LRUKNode& node)
{
    return LRUKAge{ node.fid_, node.history_.front(),( node.hCount_ < k_) ? std::nullopt : std::optional<size_t>(node.history_.back()) };
}

void LRUKReplacer::on_updatedEvictable( const LRUKNode& node )
{
    update_eviction_.emplace( node.fid_, fromNode( node ) );
}

}  // namespace bustub
