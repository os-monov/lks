import { Injectable } from '@nestjs/common';

@Injectable()
export class S3Bucket {
  constructor(url) {
    console.log(url);
    // this.url = url
  }

  // fsync to local path
  putObject() {}
}
