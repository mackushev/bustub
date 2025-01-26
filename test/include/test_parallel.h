#pragma once 

#include <iostream>
#include <thread>
#include <vector>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <atomic>
#include <exception>
#include <stdexcept>

namespace testing {

using namespace std::chrono_literals;

class TestParallelExecutor {
    public:
        using TestFunction = std::function<void()>;

        struct TestResult {
            bool success{false};
            std::string error_message;
        };

        TestParallelExecutor() {}

        // add test 
        void AddTest(TestFunction test, int count) {
            for (int i = 0; i < count; ++i) {
                functions.emplace_back(test);
            }
        }

        // run all threads in a loop
        template<class Duration = std::chrono::seconds >
        bool Run( Duration period = 1s )
        {
            std::vector<std::thread> threads;
            
            // create threads
            for ( auto test: functions ) {

                threads.emplace_back( [this, test, period]() {
                    this->threadFunc( test, std::chrono::duration_cast<std::chrono::milliseconds>( period ) );
                }  );
            };
            
            // start all
            ready_.store(true);
            start_.notify_all();

            // wait all
            for ( auto& t: threads ) {
                if ( t.joinable() ) {
                    t.join();
                }
            }
            threads.clear();

            return succeeded_.load();
        }
        
        auto Count() const { return run_counter_.load(); }

    private:
        std::mutex start_mutex_;
        std::condition_variable start_;
        std::atomic_bool ready_{false};
        std::atomic_bool stop_{false};
        std::atomic_bool succeeded_{true};
        std::atomic_int64_t run_counter_{0};
        std::atomic<size_t> completed{0};
        std::vector<TestFunction> functions;
        

        void threadFunc( TestFunction test, std::chrono::milliseconds timeout )
        {
            wait_start();
            auto start = std::chrono::steady_clock::now();
            try {
                while (! isStopped() ) {
                    
                    test();

                    run_counter_.fetch_add( 1 );

                    auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start).count();
                    
                    if ( diff > timeout.count() ) {
                        break;
                    }
                }
            } catch ( const std::exception& e ) {
                
                putResult( false, e.what() );
                stopAll();
                return;
            } catch ( ... ) {
                
                putResult( false, "Unknown error" );
                stopAll();
                return;
            };
            
            putResult( true );
        }

        void putResult( bool success, std::string message = "" )
        {
            completed++;
            if ( not success ) {
                succeeded_.store( false );
                std::cout << "Test error: " << message;
            }
        }

        bool isStopped() const 
        {
            return stop_.load();
        }

        void stopAll( )
        {
            stop_.store(true);
        }

        void wait_start()
        {
            std::unique_lock lock( start_mutex_ );
            start_.wait( lock, [this](){ return this->ready_.load(); } );
        }
};

// randomizer helper
class randomizer {
  public:
  randomizer(int n) : 
    gen(  rd() ),
    distrib(0, n-1)
  {

  }

  int random() {
    return distrib(gen);
  }

  private:
  std::random_device rd;  
  std::mt19937 gen;
  std::uniform_int_distribution<> distrib;

};


} // namespace testing

