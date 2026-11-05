(function (global) {
  "use strict";

  const SDK_VERSION = "1.0.0";

  function uuid() {
    if (global.crypto && global.crypto.randomUUID) return global.crypto.randomUUID();
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (char) {
      const value = Math.random() * 16 | 0;
      return (char === "x" ? value : (value & 3 | 8)).toString(16);
    });
  }

  class AbSdk {
    constructor() {
      this.apiBase = "";
      this.userId = "";
      this.sessionId = "";
      this.assignments = new Map();
    }

    init(options) {
      const config = options || {};
      this.apiBase = (config.apiBase || "").replace(/\/$/, "");
      this.userId = config.userId || localStorage.getItem("ab_user_id") || uuid();
      this.sessionId = sessionStorage.getItem("ab_session_id") || uuid();
      localStorage.setItem("ab_user_id", this.userId);
      sessionStorage.setItem("ab_session_id", this.sessionId);
      return this;
    }

    async getVariant(experimentKey) {
      if (!this.userId) throw new Error("请先调用 AbSdk.init()");
      if (this.assignments.has(experimentKey)) return this.assignments.get(experimentKey);

      const response = await fetch(`${this.apiBase}/api/assignments`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          experiment_key: experimentKey,
          user_id: this.userId
        })
      });
      const assignment = await response.json();
      if (!response.ok) throw new Error(assignment.detail || "实验分流失败");
      this.assignments.set(experimentKey, assignment);

      if (assignment.in_experiment) {
        const exposureKey = `ab_exposure:${this.sessionId}:${assignment.experiment_id}`;
        if (!sessionStorage.getItem(exposureKey)) {
          await this.trackWithAssignment("exposure", {}, assignment);
          sessionStorage.setItem(exposureKey, "1");
        }
      }
      return assignment;
    }

    async track(eventName, properties, experimentKey) {
      const assignment = await this.getVariant(experimentKey);
      if (!assignment.in_experiment) return { accepted: 0, event_ids: [] };
      return this.trackWithAssignment(eventName, properties || {}, assignment);
    }

    async trackWithAssignment(eventName, properties, assignment) {
      const event = {
        event_id: uuid(),
        event_name: eventName,
        event_time: new Date().toISOString(),
        user_id: this.userId,
        session_id: this.sessionId,
        experiment_id: assignment.experiment_id,
        variant_id: assignment.variant_id,
        properties: properties || {},
        sdk_version: SDK_VERSION,
        page_url: global.location.href
      };
      const response = await fetch(`${this.apiBase}/api/events`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(event),
        keepalive: true
      });
      const body = await response.json();
      if (!response.ok) throw new Error(body.detail || "埋点上报失败");
      return body;
    }
  }

  global.AbSdk = new AbSdk();
})(window);
