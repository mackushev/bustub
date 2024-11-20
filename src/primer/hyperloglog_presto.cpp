#include "primer/hyperloglog_presto.h"

namespace bustub {


static  
auto ExtractValue( hash_t hash, u_int8_t n_leading_bits) -> std::tuple<size_t, u_int8_t>
{
  // bucket 
  const auto value_capacity = static_cast<size_t>( BITSET_CAPACITY - n_leading_bits );
  const size_t bucket = value_capacity < BITSET_CAPACITY ? (hash >> value_capacity) : 0;

  // value 
  u_int8_t pos = 0;
  hash_t check = 1;
  for (; pos < value_capacity; pos++ ) {
    if ( (hash & check) != 0 ) {
      break;
    }
    check <<= 1;
  }
  return std::make_pair(bucket, pos);
}

template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0), n_leading_bits_(std::max<int16_t>(0, n_leading_bits))
{
  dense_bucket_.resize( std::pow( 2, n_leading_bits_) );
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  
  size_t buket = 0;
  u_int8_t value = 0;

  std::tie( buket, value ) = ExtractValue(CalculateHash(val), n_leading_bits_);

  if ( value > GetValue( buket ) ) {
    PutValue(buket, value);
  }

}

template <typename T>
auto HyperLogLogPresto<T>::GetValue( size_t bucket ) const -> u_int8_t 
{
    u_int8_t result = dense_bucket_[ bucket ].to_ulong();

    auto pos = overflow_bucket_.find( bucket );
    if ( pos != overflow_bucket_.end() ) {
      result += ( pos->second.to_ulong() << DENSE_BUCKET_SIZE  );      
    }

    return result;;
}

template <typename T>
auto HyperLogLogPresto<T>::PutValue( size_t bucket, u_int8_t value ) -> void 
{
  dense_bucket_[ bucket ] = std::bitset<DENSE_BUCKET_SIZE>{value}; 
  const u_int8_t overflow = ( static_cast<u_int8_t>(value) >> DENSE_BUCKET_SIZE );

  if ( overflow != 0 ) {
    overflow_bucket_[bucket] = std::bitset<OVERFLOW_BUCKET_SIZE>{overflow };
  }

}



template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {

  double devider = .0;

  for ( size_t i = 0; i < dense_bucket_.size(); i++ ) {
    devider += std::pow<double>( 2, -GetValue(i) );
  }
  cardinality_ = std::floor( CONSTANT * std::pow<double>( dense_bucket_.size(), 2) / devider );
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
