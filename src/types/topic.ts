import { IsDate, IsString } from 'class-validator';

/**
 * Basic definition for a topic.
 */
export default class Topic {
  @IsString()
  id: String;

  @IsString()
  name: String;

  @IsDate()
  updatedTimestamp: Date;

  @IsDate()
  createdTimestamp: Date;
}
