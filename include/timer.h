#pragma once

struct timer
{
        timespec start_ts, stop_ts;

        void start()
        {
                clock_gettime(CLOCK_REALTIME, &start_ts);
        }
        void stop()
        {
                clock_gettime(CLOCK_REALTIME, &stop_ts);
        }
        int get_time_ms()
        {
                size_t _start = static_cast<size_t>(start_ts.tv_sec * 1000000000 + start_ts.tv_nsec);
                size_t _stop = static_cast<size_t>(stop_ts.tv_sec * 1000000000 + stop_ts.tv_nsec);
                return static_cast<int>(static_cast<double>(_stop - _start) / 1000. / 1000.);
        }
};
