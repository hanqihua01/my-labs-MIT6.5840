# 1. MapReduce
## Paper Notes
### Programming Model
Map接收key/value，通常key是文件名，value是文件内容，对value处理后产生大量key/value，称为中间key/value；Reduce接收中间key/value，对相同key的所有value进行处理，产生结果key/value。
### Implementation
![Alt text](img/image.png)
#### 执行过程
1. 用户将输入数据划分为M片；
2. Master分派M个任务给workers，一个worker上可能有多个任务；
3. 每个map任务对自己的分片进行map操作，产生中间key/value；
4. 每个map任务将中间key/value进行分桶，分为R桶，通常采用哈希操作，由于所有任务采用相同哈希函数，因此不同任务产生的相同编号的桶中的内容通常是相同或相近的；
5. 每个reduce任务从所有map任务的存储中查询自己对应编号的桶，然后对所有桶一起进行排序；
6. 之后可以对排好序的key/value顺序进行reduce操作，将输出key/value写入存储。
#### 容错
worker容错：master周期性ping所有workers，如果无响应，对于已经完成map任务的，把该任务交给其他worker重做，对于正在执行的map或reduce任务，也重做。已经完成map任务需要重做是因为map得到的中间key/value存在本机上，无响应则无法获取，而已经完成的reduce任务无需重做是因为reduce得到的结果存在全局共享的文件系统内。

master容错：master设置周期性的checkpoint，出错后更换master从checkpoint继续。但是master仅有一个节点，出错的概率非常小，所以谷歌并没有实现master的容错。
#### 局部性
由于不同文件存储在不同机器上，可以指定存储待执行文件的机器去执行map操作，从而减少延迟。
#### 任务粒度
M和R一般比机器数量要大，从而实现负载均衡，并且在节点失效后可以加速恢复：把不同的任务分配给其他不同的节点。
#### 备份任务
为了避免单台机器执行过慢形成短板，当MapReduce快结束时，master将仍在执行的任务分配给其他空闲机器，任何一台机器执行完任务，该任务就被认为执行结束。
## 代码实现
### 优点总结
#### 容错（可用性）如何体现？
在coordinator分配给worker任务之后，会在10秒后检查任务是否完成，如果未完成，将该任务标记为未完成从而待分配。
#### 负载均衡（性能）如何体现？
worker通过rpc向coordinator申请任务，实现了动态负载均衡；中间key/value通过哈希划分为大致均衡的桶，每个reduce worker执行所有map worker上的同一编号的桶，从而保证相对均衡的工作量。
#### 可扩展性（性能）如何体现？
通过改变map节点和reduce节点的数量，可以实现可扩展性。
#### （一致性）如何体现？
coordinator分配任何和通过rpc接收到任务完成消息时，需要先加锁，再对结构体进行操作。
### 改进空间
#### 任务粒度并不完美
每个执行map操作的worker处理一个输入文件，M和机器数量相等，粒度划分并不优秀。
#### 分布式执行会带来新的问题
map任务产生的中间文件都保存在本地，reduce也从本地根据文件名规则读文件，但如果分布式执行，一个map节点挂掉后，coordinator必须通知所有reduce节点新的map在哪里执行，从而保证reduce节点能读到正确的中间文件。
# 2. GFS
### What are the steps when client C wants to read a file?
1. C sends filename and offset to coordinator (CO) (if not cached) 

    CO has a filename -> array-of-chunkhandle table

    and a chunkhandle -> list-of-chunkservers table
2. CO finds chunk handle for that offset
3. CO replies with chunkhandle + list of chunkservers
4. C caches handle + chunkserver list
5. C sends request to nearest chunkserver
   
   chunk handle, offset
6. chunk server reads from chunk file on disk, returns to client
### What are the steps when C wants to write a file at some offset?
1. C asks CO about file's chunk @ offset
2. CO tells C the primary and secondaries
3. C sends data to all (just temporary...),waits for all replies (?)
4. C asks P to write
5. P checks that lease hasn't expired
6. P writes its own chunk file (a Linux file)
7. P tells each secondary to write (copytemporary into chunk file)
8. P waits for all secondaries to reply, or timeout
   
   secondary can reply "error" e.g. out of disk space
9. P tells C "ok" or "error"
10. C retries from start if error
### Pros
#### 可扩展性（性能）与负载均衡（性能）
将naming（coordinator）与storage（chunkserver）分开，chunkserver节点数量可以增加，从而实现可扩展性，当多个clients向多个chunkserver请求访问时，总体throughput得到极大提升。

coordinator节点管理文件chunk划分，由于该节点只有一个，掌握全局信息，所以可以智慧地实现负载均衡。
#### 容错（可用性）
每个chunk保存在三个chunkserver上，一个primary，两个secondary。

primary由coordinator指定，通过lease方式，指定某个副本为primary的时间为60秒，使用lease是非常聪明的选择，因为如果不采用lease，primary挂掉，coordinator无法分辨究竟是primary挂掉了还是网络延迟了，如果此时coordinator指定另一个副本为primary，那么就可能存在两个primary，如果不指定新的primary，可能真的挂掉了，所以采用了lease以后，每到60秒，原先的primary如果没有挂掉，也会自动放弃primary身份，coordinator可以指定新的primary。

如果secondary挂掉，coordinator指定新的chunkserver存储chunk副本。
#### （一致性）
读文件，可以从primary或secondary读，写文件，只能从primary写，然后primary对secondary进行同步。
### Cons
#### 单个coordinator
单个coordinator可能会因为CPU、内存资源耗尽等问题而挂掉，造成严重的单点故障；即便没有挂掉，单个coordinator也可能称为性能瓶颈。
#### 小文件不友好
chunk大小是64MB，因此chunkserver存储小文件时并不高效。
#### 缺少fail-over
当primary挂掉，coordinator需要等到lease到期才会指定新的primary，所以效率较低。
#### 一致性过弱
GFS采用的是弱一致性，也许太弱了~