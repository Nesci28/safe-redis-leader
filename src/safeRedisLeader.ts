import { BinaryLike, createHash, randomBytes } from "crypto";
import IORedis from "ioredis";

import { atomicGetIsEqualDelete } from "./atomicGetIsEqualDelete";
import { atomicGetIsEqualSetPExpire } from "./atomicGetIsEqualSetPExpire";
import { MainEmitter } from "./emitter";

export enum EmitterEnum {
  NOT_ELECTED = "notElected",
  ELECTED = "elected",
  DEMOTED = "demoted",
  ERROR = "error",
  RENEW = "renew",
}

const hashKey = (key: BinaryLike): string => `leader: ${createHash("sha1").update(key).digest("hex")}`;

const random = (): string => randomBytes(32).toString("base64");

export class SafeRedisLeader {
  public isStarted = false;

  public wasLeading = false;

  public canLead = false;

  public emitter = new MainEmitter();

  public id = hashKey(random());

  public renewTimeoutId!: ReturnType<typeof setTimeout>;

  public electTimeoutId!: ReturnType<typeof setTimeout>;

  public asyncRedis: IORedis.Redis;

  public ttl: number;

  public wait: number;

  public key: IORedis.KeyType;

  constructor(
    asyncRedis: IORedis.Redis,
    ttl: number,
    wait: number,
    key: IORedis.KeyType,
  ) {
    this.asyncRedis = asyncRedis;
    this.ttl = ttl;
    this.wait = wait;
    this.key = hashKey(key || random());
  }

  public async renew(): Promise<void> {
    try {
      const isLeading = await atomicGetIsEqualSetPExpire(
        this.asyncRedis,
        this.key,
        this.id,
        this.ttl,
      );

      if (isLeading) {
        this.wasLeading = true;
        this.renewTimeoutId = setTimeout(this.renew.bind(this), this.ttl / 2);
        this.emitter.emit(EmitterEnum.RENEW);
      } else {
        if (this.wasLeading) {
          this.wasLeading = false;
          this.emitter.emit(EmitterEnum.DEMOTED);
        }
        clearTimeout(this.renewTimeoutId);
        this.electTimeoutId = setTimeout(this.runElection.bind(this), this.wait);
      }
    } catch (err) {
      if (this.isStarted) {
        this.emitter.emit(EmitterEnum.ERROR, err);
      }
    }
  }

  public async runElection(): Promise<void> {
    try {
      const res = await this.asyncRedis.set(this.key, this.id, "PX", this.ttl, "NX");
      if (res) {
        this.emitter.emit(EmitterEnum.ELECTED);
        this.wasLeading = true;
        if (!this.canLead) {
          return this.stop();
        }
        this.renewTimeoutId = setTimeout(this.renew.bind(this), this.ttl / 2);
      } else {
        this.emitter.emit(EmitterEnum.NOT_ELECTED);
        this.electTimeoutId = setTimeout(this.runElection.bind(this), this.wait);
      }
    } catch (err) {
      if (this.isStarted) {
        this.emitter.emit(EmitterEnum.ERROR, err);
      }
    }
  }

  public async elect(): Promise<void> {
    this.isStarted = true;
    this.canLead = true;
    await this.runElection();
  }

  public async isLeader(): Promise<boolean> {
    const curId = await this.asyncRedis.get(this.key);

    return this.id === curId;
  }

  public async stop(): Promise<void> {
    this.canLead = false;
    this.renewTimeoutId && clearTimeout(this.renewTimeoutId);
    this.electTimeoutId && clearTimeout(this.electTimeoutId);
    const res = await atomicGetIsEqualDelete(this.asyncRedis, this.key, this.id);

    if (res) {
      this.emitter.emit(EmitterEnum.DEMOTED);
    }
    this.wasLeading = false;
  }

  public on(name: string, fn: () => void): void {
    this.emitter.on(name, fn);
  }

  public off(name: string, fn: () => void): void {
    this.emitter.off(name, fn);
  }

  public once(name: string, fn: () => void): void {
    this.emitter.once(name, fn);
  }

  public removeAllListeners(): void {
    this.emitter.removeAllListeners();
  }

  public async shutdown(): Promise<void> {
    this.isStarted = false;
    this.canLead = false;
    this.renewTimeoutId && clearTimeout(this.renewTimeoutId);
    this.electTimeoutId && clearTimeout(this.electTimeoutId);
    await this.stop();
  }
}
