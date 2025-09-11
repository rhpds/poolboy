import os
import cProfile
import pstats
import atexit
import logging
import signal
from datetime import datetime

class PoolboyProfiler:
    """Simple profiler for Poolboy operator controlled via signals."""
    
    def __init__(self):
        self.profiler = None
        self.profile_file = '/tmp/poolboy_profile.prof'
        self.stats_file = '/tmp/poolboy_profile_stats.txt'
        self.logger = logging.getLogger('poolboy.profiling')
        self.is_profiling = False
        
        # Setup signal handlers
        signal.signal(signal.SIGUSR1, self._signal_start_profiling)
        signal.signal(signal.SIGUSR2, self._signal_stop_profiling)
    
    def _signal_start_profiling(self, signum, frame):
        """Signal handler to start profiling (SIGUSR1)."""
        self.start_profiling()
    
    def _signal_stop_profiling(self, signum, frame):
        """Signal handler to stop profiling (SIGUSR2)."""
        self.stop_profiling()
        
    def start_profiling(self):
        """Start profiling."""
        if self.is_profiling:
            self.logger.info("Profiling already active")
            return
            
        self.logger.info(f"Starting profiling - output will be saved to {self.profile_file}")
        self.profiler = cProfile.Profile()
        self.profiler.enable()
        self.is_profiling = True
        
    def stop_profiling(self):
        """Stop profiling and save results."""
        if not self.is_profiling or not self.profiler:
            self.logger.info("Profiling not active")
            return
            
        self.profiler.disable()
        self.is_profiling = False
        
        # Save raw profile data
        self.profiler.dump_stats(self.profile_file)
        
        # Generate human-readable stats
        try:
            with open(self.stats_file, 'w') as f:
                f.write(f"Poolboy Profiling Results - Generated at {datetime.now()}\n")
                f.write("=" * 80 + "\n\n")
                
                stats = pstats.Stats(self.profiler)
                
                # Top functions by cumulative time
                f.write("TOP 50 FUNCTIONS BY CUMULATIVE TIME:\n")
                f.write("-" * 40 + "\n")
                stats.sort_stats('cumulative')
                stats.print_stats(50, file=f)
                
                f.write("\n\nTOP 50 FUNCTIONS BY TOTAL TIME:\n")
                f.write("-" * 40 + "\n")
                stats.sort_stats('tottime')
                stats.print_stats(50, file=f)
                
                f.write("\n\nFUNCTIONS WITH MOST CALLS:\n")
                f.write("-" * 40 + "\n")
                stats.sort_stats('ncalls')
                stats.print_stats(50, file=f)
                
            self.logger.info(f"Profiling stopped - results saved to {self.profile_file} and {self.stats_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving profiling stats: {e}")
    
    def cleanup(self):
        """Cleanup on shutdown - stop profiling if active."""
        if self.is_profiling:
            self.stop_profiling()

# Global profiler instance
profiler = PoolboyProfiler()