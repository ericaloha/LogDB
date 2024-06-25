#include <fcntl.h>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
int main(int argc, char *argv[]) {

  int fd = open("/tmp/example.txt", O_RDWR | O_CREAT, 0644);
  if (fd == -1) {
    perror("open");
    exit(EXIT_FAILURE);
  }
  char buf[512];
  auto n_read = pread(fd, buf, 512, 0);
//  printf("read %zu bytes.\n", n_read);

  auto n_write = pwrite(fd, buf, 512, 0);
//  printf("write %zu bytes.\n", n_write);
  close(fd);

  return 0;
}
