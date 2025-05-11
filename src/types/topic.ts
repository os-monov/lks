import { IsDate, IsString } from 'class-validator';

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
