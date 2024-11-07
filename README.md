# Lads

Lads is an open-source project implementing "log-as-database" for MySQL in the disaggregated storage system.

Lads mainly includes two categories: (1) a client module in the computation node for kernel level cache implementation, table filter, and NFS client communication; (2) a server module in the storage node for log offloading implementation.

Lads is built atop MySQL 5.7.30. It eliminates networked page flush operations by storage node local page regeneration.

For the setup and deployment guide, please refer to DeployGuide.pdf for details. 
