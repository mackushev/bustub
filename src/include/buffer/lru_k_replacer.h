//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <atomic>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <queue>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
  private:

  LRUKNode(frame_id_t id, size_t access_t) : fid_(id) {
    history_.push_front( access_t );
  }

  const frame_id_t fid_;
  std::list<size_t> history_;
  size_t hCount_{1};
  bool is_evictable_{false};

  friend class LRUKReplacer;
};

struct LRUKAge {

  bool operator<(const LRUKAge& other) const;
  
  frame_id_t fid_;
  size_t lAccess_;
  std::optional<size_t> kAccess_;
};


/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class   LRUKReplacer {
 public:
  LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  auto Size() -> size_t;

 private:

  [[maybe_unused]] size_t curr_size_{0};
  [[maybe_unused]] size_t replacer_size_{0};
  
  // history size
  const size_t k_;
  // current timestamp,  
  std::atomic<size_t> current_timestamp_{0};

  // lock 
  std::mutex latch_store_;
  std::mutex latch_evictable_;
  // stores all not removed or evicted nodes info (with history)
  std::unordered_map<frame_id_t, LRUKNode> node_store_;

  // Evicted heap
  std::unordered_map<frame_id_t, LRUKAge> update_eviction_;
  std::vector<LRUKAge> evictable_;

  void removeEvictable( frame_id_t node );
  void addEvictable( const LRUKNode& node );
  void on_updatedEvictable( const LRUKNode& node );
  LRUKAge fromNode( const LRUKNode& node);
};

}  // namespace bustub
