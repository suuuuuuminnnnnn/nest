import { expect } from 'chai';
import { EMPTY } from 'rxjs';
import * as sinon from 'sinon';
import { ClientMqtt } from '../../client/client-mqtt';
import { MqttEventsMap } from '../../events/mqtt.events';
import { ReadPacket } from '../../interfaces';
import { MqttRecord } from '../../record-builders';

describe('ClientMqtt', () => {
  const test = 'test';
  let client: ClientMqtt = new ClientMqtt({});
  let untypedClient = client as any;

  describe('getRequestPattern', () => {
    it(`should leave pattern as it is`, () => {
      expect(client.getRequestPattern(test)).to.equal(test);
    });
  });
  describe('getResponsePattern', () => {
    it(`should append "/reply" to string`, () => {
      const expectedResult = test + '/reply';
      expect(client.getResponsePattern(test)).to.equal(expectedResult);
    });
  });
  describe('publish', () => {
    const pattern = 'test';
    let msg: ReadPacket;
    let subscribeSpy: sinon.SinonSpy,
      publishSpy: sinon.SinonSpy,
      onSpy: sinon.SinonSpy,
      removeListenerSpy: sinon.SinonSpy,
      unsubscribeSpy: sinon.SinonSpy,
      connectSpy: sinon.SinonStub,
      assignStub: sinon.SinonStub,
      mqttClient: any;

    const id = '1';
    beforeEach(() => {
      client = new ClientMqtt({});
      untypedClient = client as any;

      msg = { pattern, data: 'data' };
      subscribeSpy = sinon.spy((name, fn) => fn());
      publishSpy = sinon.spy();
      onSpy = sinon.spy();
      removeListenerSpy = sinon.spy();
      unsubscribeSpy = sinon.spy();

      mqttClient = {
        subscribe: subscribeSpy,
        on: (type, handler) => (type === 'subscribe' ? handler() : onSpy()),
        removeListener: removeListenerSpy,
        unsubscribe: unsubscribeSpy,
        publish: publishSpy,
        addListener: () => ({}),
      };
      untypedClient.mqttClient = mqttClient;
      connectSpy = sinon.stub(client, 'connect');
      assignStub = sinon
        .stub(client, 'assignPacketId' as any)
        .callsFake(packet => Object.assign(packet as object, { id }));
    });
    afterEach(() => {
      connectSpy.restore();
      assignStub.restore();
    });
    it('should subscribe to response pattern name', async () => {
      client['publish'](msg, () => {});
      expect(subscribeSpy.calledWith(`${pattern}/reply`)).to.be.true;
    });
    it('should publish stringified message to request pattern name', async () => {
      client['publish'](msg, () => {});
      expect(publishSpy.calledWith(pattern, JSON.stringify(msg))).to.be.true;
    });
    it('should add callback to routing map', async () => {
      client['publish'](msg, () => {});
      expect(client['routingMap'].has(id)).to.be.true;
    });
    describe('on error', () => {
      beforeEach(() => {
        assignStub.callsFake(() => {
          throw new Error();
        });
      });

      it('should call callback', () => {
        const callback = sinon.spy();
        client['publish'](msg, callback);

        expect(callback.called).to.be.true;
        expect(callback.getCall(0).args[0].err).to.be.instanceof(Error);
      });
    });
    describe('dispose callback', () => {
      let getResponsePatternStub: sinon.SinonStub;
      let callback: sinon.SinonSpy, subscription;

      const channel = 'channel';

      beforeEach(async () => {
        callback = sinon.spy();

        getResponsePatternStub = sinon
          .stub(client, 'getResponsePattern')
          .callsFake(() => channel);
        subscription = client['publish'](msg, callback);
        subscription(channel, JSON.stringify({ isDisposed: true, id }));
      });
      afterEach(() => {
        getResponsePatternStub.restore();
      });

      it('should unsubscribe to response pattern name', () => {
        expect(unsubscribeSpy.calledWith(channel)).to.be.true;
      });
      it('should remove callback from routing map', () => {
        expect(client['routingMap'].has(id)).to.be.false;
      });
    });
    describe('headers', () => {
      it('should not generate headers if none are configured', async () => {
        client['publish'](msg, () => {});
        expect(publishSpy.getCall(0).args[2]).to.be.undefined;
      });
      it('should send packet headers', async () => {
        const requestHeaders = { '1': '123' };
        msg.data = new MqttRecord('data', {
          properties: { userProperties: requestHeaders },
        });

        client['publish'](msg, () => {});
        expect(publishSpy.getCall(0).args[2].properties.userProperties).to.eql(
          requestHeaders,
        );
      });
      it('should combine packet and static headers', async () => {
        const staticHeaders = { 'client-id': 'some-client-id' };
        untypedClient.options.userProperties = staticHeaders;

        const requestHeaders = { '1': '123' };
        msg.data = new MqttRecord('data', {
          properties: { userProperties: requestHeaders },
        });

        client['publish'](msg, () => {});
        expect(publishSpy.getCall(0).args[2].properties.userProperties).to.eql({
          ...staticHeaders,
          ...requestHeaders,
        });
      });
      it('should prefer packet headers over static headers', async () => {
        const staticHeaders = { 'client-id': 'some-client-id' };
        untypedClient.options.headers = staticHeaders;

        const requestHeaders = { 'client-id': 'override-client-id' };
        msg.data = new MqttRecord('data', {
          properties: { userProperties: requestHeaders },
        });

        client['publish'](msg, () => {});
        expect(publishSpy.getCall(0).args[2].properties.userProperties).to.eql(
          requestHeaders,
        );
      });
    });
  });
  describe('createResponseCallback', () => {
    let callback: sinon.SinonSpy, subscription;
    const responseMessage = {
      response: 'test',
      id: '1',
    };

    describe('not completed', () => {
      beforeEach(async () => {
        callback = sinon.spy();
        subscription = client.createResponseCallback();

        client['routingMap'].set(responseMessage.id, callback);
        subscription('channel', Buffer.from(JSON.stringify(responseMessage)));
      });
      it('should call callback with expected arguments', () => {
        expect(
          callback.calledWith({
            err: undefined,
            response: responseMessage.response,
          }),
        ).to.be.true;
      });
    });
    describe('disposed and "id" is correct', () => {
      beforeEach(async () => {
        callback = sinon.spy();
        subscription = client.createResponseCallback();

        client['routingMap'].set(responseMessage.id, callback);
        subscription(
          'channel',
          Buffer.from(
            JSON.stringify({
              ...responseMessage,
              isDisposed: true,
            }),
          ),
        );
      });

      it('should call callback with dispose param', () => {
        expect(callback.called).to.be.true;
        expect(
          callback.calledWith({
            isDisposed: true,
            response: responseMessage.response,
            err: undefined,
          }),
        ).to.be.true;
      });
    });
    describe('disposed and "id" is incorrect', () => {
      beforeEach(async () => {
        callback = sinon.spy();
        subscription = client.createResponseCallback();

        client['routingMap'].set('3', callback);
        subscription('channel', Buffer.from(JSON.stringify(responseMessage)));
      });

      it('should not call callback', () => {
        expect(callback.called).to.be.false;
      });
    });
  });
  describe('close', () => {
    let endSpy: sinon.SinonSpy;
    beforeEach(() => {
      endSpy = sinon.spy();
      untypedClient.mqttClient = { endAsync: endSpy };
    });
    it('should close "pub" when it is not null', async () => {
      await client.close();
      expect(endSpy.called).to.be.true;
    });
    it('should not close "pub" when it is null', async () => {
      untypedClient.mqttClient = null;
      await client.close();
      expect(endSpy.called).to.be.false;
    });
  });
  describe('connect', () => {
    let createClientStub: sinon.SinonStub;
    let registerErrorListenerSpy: sinon.SinonSpy;
    let connect$Stub: sinon.SinonStub;
    let mergeCloseEvent: sinon.SinonStub;

    beforeEach(async () => {
      createClientStub = sinon.stub(client, 'createClient').callsFake(
        () =>
          ({
            addListener: () => ({}),
            removeListener: () => ({}),
            on: () => ({}),
          }) as any,
      );
      registerErrorListenerSpy = sinon.spy(client, 'registerErrorListener');
      connect$Stub = sinon.stub(client, 'connect$' as any).callsFake(() => ({
        subscribe: ({ complete }) => complete(),
        pipe() {
          return this;
        },
      }));
      mergeCloseEvent = sinon
        .stub(client, 'mergeCloseEvent')
        .callsFake((_, source) => source);
    });
    afterEach(() => {
      createClientStub.restore();
      registerErrorListenerSpy.restore();
      connect$Stub.restore();
      mergeCloseEvent.restore();
    });
    describe('when is not connected', () => {
      beforeEach(async () => {
        client['mqttClient'] = null;
        await client.connect();
      });
      it('should call "registerErrorListener" once', async () => {
        expect(registerErrorListenerSpy.called).to.be.true;
      });
      it('should call "createClient" once', async () => {
        expect(createClientStub.called).to.be.true;
      });
      it('should call "connect$" once', async () => {
        expect(connect$Stub.called).to.be.true;
      });
    });
    describe('when is connected', () => {
      beforeEach(() => {
        client['mqttClient'] = { test: true } as any;
      });
      it('should not call "createClient"', () => {
        expect(createClientStub.called).to.be.false;
      });
      it('should not call "registerErrorListener"', () => {
        expect(registerErrorListenerSpy.called).to.be.false;
      });
      it('should not call "connect$"', () => {
        expect(connect$Stub.called).to.be.false;
      });
    });
  });
  describe('mergeCloseEvent', () => {
    it('should merge close event', () => {
      const error = new Error();
      const instance: any = {
        on: (ev, callback) => callback(error),
        off: () => ({}),
      };
      client.mergeCloseEvent(instance, EMPTY).subscribe({
        error: (err: any) => expect(err).to.be.eql(error),
      });
    });
  });
  describe('registerErrorListener', () => {
    it('should bind error event handler', () => {
      const callback = sinon.stub().callsFake((_, fn) => fn({ code: 'test' }));
      const emitter = {
        on: callback,
      };
      client.registerErrorListener(emitter as any);
      expect(callback.getCall(0).args[0]).to.be.eql(MqttEventsMap.ERROR);
    });
  });
  describe('registerConnectListener', () => {
    it('should bind connect event handler', () => {
      const callback = sinon.stub().callsFake((_, fn) => fn({ code: 'test' }));
      const emitter = {
        on: callback,
      };
      client.registerConnectListener(emitter as any);
      expect(callback.getCall(0).args[0]).to.be.eql(MqttEventsMap.CONNECT);
    });
  });
  describe('registerDisconnectListener', () => {
    it('should bind disconnect event handler', () => {
      const callback = sinon.stub().callsFake((_, fn) => fn({ code: 'test' }));
      const emitter = {
        on: callback,
      };
      client.registerDisconnectListener(emitter as any);
      expect(callback.getCall(0).args[0]).to.be.eql(MqttEventsMap.DISCONNECT);
    });
  });
  describe('registerOfflineListener', () => {
    it('should bind offline event handler', () => {
      const callback = sinon.stub().callsFake((_, fn) => fn({ code: 'test' }));
      const emitter = {
        on: callback,
      };
      client.registerOfflineListener(emitter as any);
      expect(callback.getCall(0).args[0]).to.be.eql(MqttEventsMap.OFFLINE);
    });
  });
  describe('registerCloseListener', () => {
    it('should bind close event handler', () => {
      const callback = sinon.stub().callsFake((_, fn) => fn({ code: 'test' }));
      const emitter = {
        on: callback,
      };
      client.registerCloseListener(emitter as any);
      expect(callback.getCall(0).args[0]).to.be.eql(MqttEventsMap.CLOSE);
    });
  });
  describe('dispatchEvent', () => {
    let msg: ReadPacket;
    let publishStub: sinon.SinonStub, mqttClient;

    beforeEach(() => {
      client = new ClientMqtt({});
      untypedClient = client as any;

      msg = { pattern: 'pattern', data: 'data' };
      publishStub = sinon.stub();
      mqttClient = {
        publish: publishStub,
      };
      untypedClient.mqttClient = mqttClient;
    });

    it('should publish packet', async () => {
      publishStub.callsFake((a, b, c, d) => d());
      await client['dispatchEvent'](msg);

      expect(publishStub.called).to.be.true;
    });
    it('should throw error', async () => {
      publishStub.callsFake((a, b, c, d) => d(new Error()));
      client['dispatchEvent'](msg).catch(err =>
        expect(err).to.be.instanceOf(Error),
      );
    });
    describe('headers', () => {
      it('should not generate headers if none are configured', async () => {
        publishStub.callsFake((a, b, c, d) => d());
        await client['dispatchEvent'](msg);
        expect(publishStub.getCall(0).args[2]).to.be.undefined;
      });
      it('should send packet headers', async () => {
        publishStub.callsFake((a, b, c, d) => d());
        const requestHeaders = { '1': '123' };
        msg.data = new MqttRecord('data', {
          properties: { userProperties: requestHeaders },
        });

        await client['dispatchEvent'](msg);
        expect(publishStub.getCall(0).args[2].properties.userProperties).to.eql(
          requestHeaders,
        );
      });
      it('should combine packet and static headers', async () => {
        publishStub.callsFake((a, b, c, d) => d());
        const staticHeaders = { 'client-id': 'some-client-id' };
        untypedClient.options.userProperties = staticHeaders;

        const requestHeaders = { '1': '123' };
        msg.data = new MqttRecord('data', {
          properties: { userProperties: requestHeaders },
        });

        await client['dispatchEvent'](msg);
        expect(publishStub.getCall(0).args[2].properties.userProperties).to.eql(
          {
            ...staticHeaders,
            ...requestHeaders,
          },
        );
      });
      it('should prefer packet headers over static headers', async () => {
        publishStub.callsFake((a, b, c, d) => d());
        const staticHeaders = { 'client-id': 'some-client-id' };
        untypedClient.options.headers = staticHeaders;

        const requestHeaders = { 'client-id': 'override-client-id' };
        msg.data = new MqttRecord('data', {
          properties: { userProperties: requestHeaders },
        });

        await client['dispatchEvent'](msg);
        expect(publishStub.getCall(0).args[2].properties.userProperties).to.eql(
          requestHeaders,
        );
      });
    });
  });
  describe('emit race condition (issue #16302)', () => {
    let client: ClientMqtt;
    let untypedClient: any;
    let mqttClient: any;
    let publishSpy: sinon.SinonSpy;
    let publishCallbacks: Array<{
      topic: string;
      payload: any;
      options: any;
      callback: Function;
    }>;
    let connectStub: sinon.SinonStub;

    beforeEach(() => {
      client = new ClientMqtt({});
      untypedClient = client as any;
      publishCallbacks = [];

      // Mock publish to capture calls but delay execution
      publishSpy = sinon.spy(
        (topic: string, payload: any, options: any, callback: Function) => {
          // Store callback for later execution
          publishCallbacks.push({ topic, payload, options, callback });
        },
      );

      mqttClient = {
        publish: publishSpy,
        subscribe: (channel: string, cb: Function) => cb(),
        unsubscribe: sinon.spy(),
        on: sinon.spy(),
        addListener: sinon.spy(),
        removeListener: sinon.spy(),
      };
      untypedClient.mqttClient = mqttClient;

      // Stub connect to return immediately
      connectStub = sinon.stub(client, 'connect').resolves();
    });

    afterEach(() => {
      connectStub.restore();
    });

    it('should publish to the correct topic per emit call', async () => {
      const topics = [
        'cmd/device/123/reboot',
        'cmd/device/456/update',
        'cmd/device/789/status',
      ];
      const payloads = ['data1', 'data2', 'data3'];

      // Rapidly emit to different topics
      const observables = topics.map((topic, index) =>
        client.emit(topic, payloads[index]),
      );

      // Wait a bit for all deferred observables to start executing
      await new Promise(resolve => setImmediate(resolve));

      // Now execute all publish callbacks to simulate async publish completion
      publishCallbacks.forEach(({ callback }) => callback());

      // Verify that each topic was published with correct data
      expect(publishSpy.callCount).to.equal(topics.length);

      // Extract actual topics from publish calls
      const publishedTopics = publishCallbacks.map(call => call.topic);

      // Check if all topics are present (order might vary due to async nature)
      topics.forEach(expectedTopic => {
        expect(publishedTopics).to.include(expectedTopic);
      });

      // More strict check: verify each call has unique topic
      const uniqueTopics = new Set(publishedTopics);
      expect(uniqueTopics.size).to.equal(topics.length);
    });

    it('should not reuse the last topic for all emits', async () => {
      const topics = ['topic/a', 'topic/b', 'topic/c'];

      // Emit to different topics without awaiting
      topics.forEach((topic, index) => {
        client.emit(topic, { index });
      });

      // Wait for all emits to queue up
      await new Promise(resolve => setImmediate(resolve));

      // Execute callbacks
      publishCallbacks.forEach(({ callback }) => callback());

      // The bug would manifest as all topics being 'topic/c' (the last one)
      const publishedTopics = publishCallbacks.map(call => call.topic);

      // Verify we have different topics, not all the same
      const lastTopic = topics[topics.length - 1];
      const allSameTopic = publishedTopics.every(t => t === lastTopic);

      expect(allSameTopic).to.be.false;
    });
  });
});
