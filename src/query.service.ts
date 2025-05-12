import { ControlPlaneService } from './control.plane.service';
import { ObjectStorageService } from './object.storage.service';
import { Message } from './types/message';
// import SortedSet from 'tlhunter-sorted-set';

export class QueryService {
    constructor(
        private readonly objectStorageService: ObjectStorageService,
        private readonly controlPlaneService: ControlPlaneService,
        // private readonly cache: Map<string, SortedSet<Message>>
    ) { }

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
