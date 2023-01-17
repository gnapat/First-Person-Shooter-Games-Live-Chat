
import sys
import subprocess


def main():
    print("Start: Pipeline")
    ch_name = sys.argv[1]
    vid = sys.argv[2]
    title = sys.argv[3]

    ret = subprocess.run(["python3", "collector.py",ch_name,vid,title])
    if ret.returncode != 0:

        return(-1)
    
    # Step Sentiment
    ret = subprocess.run(["python3", "sentiment.py",ch_name,vid,title])
    if ret.returncode != 0:

        return(-1)
    
    # Step Aggregation
    ret = subprocess.run(["python3", "aggregation.py",ch_name,vid,title])
    if ret.returncode != 0:

        return(-1)

    print("Pipeline done.")


    return(0)

if __name__ == "__main__":
    main()