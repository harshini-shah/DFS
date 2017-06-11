cd C:\Users\k2kha\work\Quarter3\MiddlewareAndDistributed\Project\GithubRepo\DFS
java -jar dfs.jar ls 127.0.0.1:
java -jar dfs.jar get 127.0.0.1:/file1.txt C:/Users/k2kha/work/Quarter3/MiddlewareAndDistributed/Project/GithubRepo/DFS/MyStorage/myDir/myFile2.txt
java -jar dfs.jar put C:/Users/k2kha/work/Quarter3/MiddlewareAndDistributed/Project/GithubRepo/DFS/MyStorage/myDir/myFile1.txt 127.0.0.1:/copiedFile.txt
java -jar dfs.jar ls 127.0.0.1: