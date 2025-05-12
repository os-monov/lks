import { ControlPlaneService } from './control.plane.service';
import { ObjectStorageService } from './object.storage.service';
import { Message } from './types/message';

export class QueryService {
  constructor(
    private readonly objectStorageService: ObjectStorageService,
    private readonly controlPlaneService: ControlPlaneService,
  ) {}

  query(topic: string, partition: number, offset: number): Message[] {
    return [];

    //   // if
    //   // files: File[] = metadataService.getFiles(topic, partition, offset)
    //   // for file in files
    //   //  for message in file.messages()
    //   //      cache.addFile(message)
    //   //
    //   // (topic, partition) => Message[]
    //   // cache.query(topic, partition, offset)
  }
}
