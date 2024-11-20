#include "primer/hyperloglog.h"

#include <numeric>

namespace bustub {

static_assert( sizeof( hash_t ) * CHAR_BIT == BITSET_CAPACITY, "ERR in bits" );

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t _n_bits) :  n_bits_( _n_bits ), cardinality_(0)
{  
  assert( n_bits_ <= BITSET_CAPACITY );
  // fill array with zeros
  buckets_.resize( std::pow( 2, n_bits_ ), 0);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> TBitset {
  return {hash };
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const TBitset &bset) const -> uint64_t {
  
  const int from = bset.size() - n_bits_ - 1;
  for ( size_t pos = from; pos >= 0; pos-- ) {
    if ( bset[pos] ) {
      return from - pos + 1;
    }
  }
  return 0;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::GetValue(const TBitset &bset) const -> uint8_t {
  const auto v = PositionOfLeftmostOne(bset);
  assert(  fmt::detail::max_value<uint8_t>() > v );
  return static_cast<uint8_t>(v);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::GetRegister(const TBitset &bset) const -> size_t {
  return ( bset >> static_cast<size_t>( bset.size() - n_bits_ ) ).to_ulong();
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {

  // 0. Get hash and its binary 
  const TBitset bits = ComputeBinary( CalculateHash( val ) );
  
  // get bucket & value
  const size_t pos = GetRegister( bits );
  const auto value = GetValue( bits );

  // insert value
  buckets_[ pos ] = std::max( buckets_[pos], value );
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
 
  const double devider = std::accumulate( buckets_.begin(), buckets_.end(), (double).0, [](auto acc, auto val) -> double { return acc + std::pow<double>( 2, -val ); } );
  cardinality_ = std::floor( CONSTANT * std::pow<double>( buckets_.size(), 2) / devider );

}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
