#!/usr/bin/env python3
"""
Simple script to analyze Poolboy profiling results.
Usage: python analyze_profile.py [profile_file]
"""

import sys
import pstats
import os

def analyze_profile(profile_file='/tmp/poolboy_profile.prof'):
    """Analyze a cProfile output file and display key metrics."""
    
    if not os.path.exists(profile_file):
        print(f"Profile file {profile_file} not found!")
        print("Make sure ENABLE_PROFILING=true is set and the operator has been running.")
        return
    
    print(f"Analyzing profile data from: {profile_file}")
    print("=" * 80)
    
    stats = pstats.Stats(profile_file)
    
    print("\nPROFILE SUMMARY:")
    print("-" * 40)
    stats.print_stats(0)  # Just show summary
    
    print("\nTOP 20 FUNCTIONS BY CUMULATIVE TIME:")
    print("-" * 40)
    stats.sort_stats('cumulative').print_stats(20)
    
    print("\nTOP 20 FUNCTIONS BY TOTAL TIME:")
    print("-" * 40)
    stats.sort_stats('tottime').print_stats(20)
    
    print("\nTOP 20 MOST CALLED FUNCTIONS:")
    print("-" * 40)
    stats.sort_stats('ncalls').print_stats(20)
    
    # Find potential hotspots - functions with high total time and many calls
    print("\nPOTENTIAL HOTSPOTS (high time + many calls):")
    print("-" * 40)
    stats.sort_stats('tottime')
    hotspots = []
    for func, (cc, nc, tt, ct, callers) in stats.stats.items():
        if tt > 0.1 and nc > 100:  # Functions taking >0.1s total time with >100 calls
            hotspots.append((func, tt, nc, tt/nc if nc > 0 else 0))
    
    hotspots.sort(key=lambda x: x[1], reverse=True)  # Sort by total time
    for func, total_time, calls, avg_time in hotspots[:15]:
        filename, line, funcname = func
        print(f"{filename}:{line}({funcname}) - {total_time:.3f}s total, {calls} calls, {avg_time:.6f}s avg")

if __name__ == '__main__':
    profile_file = sys.argv[1] if len(sys.argv) > 1 else '/tmp/poolboy_profile.prof'
    analyze_profile(profile_file)