import _SQS, { ReceiveMessageCommand, DeleteMessageCommand } from '@aws-sdk/client-sqs'
import { EventEmitter } from 'events'

export interface Events {
  message: [_SQS.Message]
  error: [Error, void | _SQS.Message]
}

export interface ListenerOptions {
  queueUrl: string
  sqsClient: _SQS.SQSClient
  pollingWaitTimeMs?: number
  visibilityTimeout?: number
  messageTimoutMs?: number
  onMessage?: (message: _SQS.Message) => Promise<void>
}

export class Listener extends EventEmitter {
  private queueUrl?: string
  private started?: boolean
  private sqsClient: _SQS.SQSClient
  private pollingWaitTimeMs?: number
  private visibilityTimeout?: number
  private onMessage?: (message: _SQS.Message) => Promise<void>
  private messageTimoutMs?: number

  constructor(options: ListenerOptions) {
    super()
    this.queueUrl = options.queueUrl
    this.started = false
    this.sqsClient = options.sqsClient
    if (options.pollingWaitTimeMs && options.pollingWaitTimeMs > 20000) throw new Error('The maximum long polling wait time is 20 seconds')
    this.pollingWaitTimeMs = options.pollingWaitTimeMs || 0
    this.onMessage = options.onMessage
    this.visibilityTimeout = options.visibilityTimeout
    this.messageTimoutMs = options.messageTimoutMs
  }

  public listen(): void {
    if (!this.started) {
      this.started = true
      this.listenForMessages()
    }
  }

  public stop(): void {
    this.started = false
  }

  emit<T extends keyof Events>(event: T | symbol, ...args: Events[T]): boolean {
    return super.emit(event, ...args)
  }

  on<T extends keyof Events>(event: T, listener: (...args: Events[T]) => void): this {
    // @ts-ignore
    return super.on(event, listener)
  }

  private async listenForMessages() {
    if (!this.started) return
    const command = new ReceiveMessageCommand({
      QueueUrl: this.queueUrl,
      VisibilityTimeout: this.visibilityTimeout,
    })

    try {
      const { Messages } = await this.sqsClient!.send(command)
      if (Messages) {
        Messages.map((msg) => this.processMessage(msg))
      }
      setTimeout(() => {
        this.listenForMessages()
      }, this.pollingWaitTimeMs)
    } catch (error) {
      console.error('SQS error while receiving messages', error)
    }
  }

  private async processMessage(message: _SQS.Message): Promise<void> {
    this.emit('message', message)
    console.log('Message received', message)
    if (this.onMessage) {
      if (this.messageTimoutMs) {
        let timeout
        const promise = new Promise((_, reject) => {
          timeout = setTimeout((): void => {
            reject(new Error('Timeout error'))
          }, this.messageTimoutMs)
        })
        try {
          await Promise.race([this.onMessage(message), promise])
          await this.deleteMessage(message)
        } catch (error) {
        } finally {
          clearTimeout(timeout)
        }
      } else {
        try {
          await this.onMessage(message)
          await this.deleteMessage(message)
        } catch (error) {
          if (error instanceof Error) {
            this.emit('error', error)
            error.message = 'Unable to invoke onMessage callback' + error.message
          }
          throw error
        }
      }
    }
  }

  private async deleteMessage(message: _SQS.Message): Promise<void> {
    const command = new DeleteMessageCommand({ QueueUrl: this.queueUrl, ReceiptHandle: message.ReceiptHandle })
    try {
      await this.sqsClient!.send(command)
    } catch (error) {
      this.emit('error', error as Error)
      if (error instanceof Error) {
        error.message = 'Unable to delete message' + error.message
      }
      throw error
    }
  }
}
