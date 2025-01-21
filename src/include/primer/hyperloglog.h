//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hyperloglog.h
//
// Identification: src/include/primer/hyperloglog.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <array>
#include <bitset>
#include <memory>
#include <mutex>  // NOLINT
#include <string>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"

/** @brief Capacity of the bitset stream. */
#define BITSET_CAPACITY 64

namespace bustub {

template <typename KeyType>
class HyperLogLog {
  /** @brief Constant for HLL. */
  static constexpr double CONSTANT = 0.79402;
  using TBitset = std::bitset<BITSET_CAPACITY>;

 public:
  /** @brief Disable default constructor. */
  HyperLogLog() = delete;

  explicit HyperLogLog(int16_t n_bits);

  /**
   * @brief Getter value for cardinality.
   *
   * @returns cardinality value
   */
  auto GetCardinality() { return cardinality_; }

  auto AddElem(KeyType val) -> void;

  auto ComputeCardinality() -> void;

 private:
  /**
   * @brief Calculates Hash of a given value.
   *
   * @param[in] val - value
   * @returns hash integer of given input value
   */
  inline auto CalculateHash(KeyType val) -> hash_t {
    Value val_obj;
    if constexpr (std::is_same<KeyType, std::string>::value) {
      val_obj = Value(VARCHAR, val);
    } else {
      val_obj = Value(BIGINT, val);
    }
    return bustub::HashUtil::HashValue(&val_obj);
  }

  auto ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY>;

  auto PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t;

  auto GetRegister(const TBitset &bset) const -> size_t;
  auto GetValue(const TBitset &bset) const -> uint8_t;

  /** @brief bucket bits  */
  const int16_t n_bits_;
  /** @brief Cardinality value. */
  size_t cardinality_;

  /** @brief bucket storage. */
  std::vector<std::uint8_t> buckets_;
};

}  // namespace bustub
