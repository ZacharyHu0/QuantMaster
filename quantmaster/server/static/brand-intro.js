(function () {
  'use strict';

  const DURATION = 8.6;
  const SESSION_KEY = 'quantmaster.brand-intro.v1';
  const config = Object.assign({mode: 'app', autoplay: false, loop: false}, window.QM_BRAND_INTRO_CONFIG || {});
  const query = new URLSearchParams(window.location.search);
  const recording = window.__recording === true;
  const reducedMotion = !recording && window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;

  const clamp = value => Math.max(0, Math.min(1, value));
  const mix = (from, to, progress) => from + (to - from) * progress;
  const expoOut = value => value >= 1 ? 1 : 1 - Math.pow(2, -10 * value);
  const easeInCubic = value => value * value * value;
  const easeInOut = value => value < .5
    ? 4 * value * value * value
    : 1 - Math.pow(-2 * value + 2, 3) / 2;
  const smooth = value => value * value * (3 - 2 * value);
  const phase = (time, start, end, easing = smooth) => {
    if (time <= start) return 0;
    if (time >= end) return 1;
    return easing((time - start) / (end - start));
  };

  function canUseSessionStorage() {
    try {
      const probe = '__qm_brand_probe__';
      sessionStorage.setItem(probe, '1');
      sessionStorage.removeItem(probe);
      return true;
    } catch (_) {
      return false;
    }
  }

  const hasSessionStorage = canUseSessionStorage();
  const forced = recording || config.mode === 'preview' || query.get('intro') === '1';
  const explicitlySkipped = query.get('intro') === '0';
  const seen = hasSessionStorage && sessionStorage.getItem(SESSION_KEY) === 'seen';
  const shouldAutoplay = !explicitlySkipped && (forced || (config.autoplay === true && !seen));

  const overlay = document.createElement('div');
  overlay.id = 'qm-brand-intro';
  overlay.className = 'qm-brand-intro';
  overlay.hidden = true;
  overlay.setAttribute('aria-hidden', 'true');
  overlay.innerHTML = `
    <svg class="qm-brand-stage" viewBox="0 0 1600 900" preserveAspectRatio="xMidYMid slice" aria-hidden="true">
      <defs>
        <clipPath id="qm-wordmark-clip"><rect id="qm-wordmark-clip-rect" x="675" y="330" width="0" height="220"/></clipPath>
      </defs>

      <g id="qm-mono-sun" class="qm-intro-mono-sun">
        <rect id="qm-mono-square" x="-42" y="-42" width="84" height="84" rx="0" class="qm-intro-white"/>
      </g>
      <rect id="qm-ground" x="0" y="470" width="1600" height="430" fill="#0d0d0d"/>

      <g id="qm-horizon" class="qm-intro-horizon">
        <path id="qm-horizon-echo" pathLength="1" class="qm-intro-stroke qm-intro-horizon-echo"
          d="M278 474 C390 455 505 482 617 468 S846 477 958 466 S1182 480 1321 461"/>
        <path id="qm-horizon-raw" pathLength="1" class="qm-intro-stroke qm-intro-horizon-raw"
          d="M278 470 C388 453 505 478 617 466 S846 475 958 464 S1182 478 1321 458"/>
        <path id="qm-horizon-still" pathLength="1" class="qm-intro-stroke qm-intro-horizon-still"
          d="M278 470 C506 466 666 473 802 468 C1004 465 1164 471 1321 464"/>
        <circle id="qm-ink-tip" r="5" class="qm-intro-white"/>
      </g>

      <g id="qm-eye" class="qm-intro-eye">
        <path class="qm-intro-stroke qm-intro-eye-lid" d="M360 470 C508 264 1092 264 1240 470"/>
        <path class="qm-intro-stroke qm-intro-eye-lid" d="M360 470 C524 650 1076 650 1240 470"/>
      </g>
      <g id="qm-iris">
        <circle id="qm-iris-ring" cx="800" cy="470" r="132" pathLength="1" class="qm-intro-stroke qm-intro-eye-ring"/>
        <circle id="qm-iris-fine" cx="800" cy="470" r="103" pathLength="1" class="qm-intro-stroke qm-intro-horizon-echo"/>
      </g>
      <g id="qm-pupil" class="qm-intro-pupil">
        <circle r="34" class="qm-intro-white"/>
        <circle r="15" fill="#0d0d0d"/>
        <circle r="6" class="qm-intro-white"/>
        <circle r="47" class="qm-intro-stroke qm-intro-pupil-ring" opacity=".42"/>
      </g>

      <g id="qm-factor" class="qm-intro-factor-core">
        <rect x="-25" y="-25" width="50" height="50" fill="#0d0d0d" stroke="#f4f7fb" stroke-width="4"/>
        <rect id="qm-factor-white" x="-15" y="-15" width="30" height="30" class="qm-intro-white"/>
        <rect id="qm-factor-blue" x="-15" y="-15" width="30" height="30" class="qm-intro-blue"/>
        <circle id="qm-factor-pulse" r="42" pathLength="1" class="qm-intro-blue-stroke" stroke-width="3"/>
      </g>

      <path id="qm-factor-trail" pathLength="1" class="qm-intro-blue-stroke" stroke-width="5"
        stroke-linecap="round" d="M1110 330 Q1285 430 800 470"/>

      <g id="qm-blue-sun" class="qm-intro-blue-sun">
        <g id="qm-bloom" class="qm-intro-bloom">
          <line x1="0" y1="-91" x2="0" y2="-147"/>
          <line x1="0" y1="91" x2="0" y2="147"/>
          <line x1="-91" y1="0" x2="-147" y2="0"/>
          <line x1="91" y1="0" x2="147" y2="0"/>
          <line x1="-68" y1="-68" x2="-106" y2="-106"/>
          <line x1="68" y1="-68" x2="106" y2="-106"/>
          <line x1="-68" y1="68" x2="-106" y2="106"/>
          <line x1="68" y1="68" x2="106" y2="106"/>
        </g>
        <rect id="qm-blue-square" x="-54" y="-54" width="108" height="108" class="qm-intro-blue"/>
        <rect id="qm-blue-square-flash" x="-54" y="-54" width="108" height="108" fill="none" stroke="#f4f7fb" stroke-width="5"/>
      </g>

      <g id="qm-final-mark" class="qm-intro-final-mark">
        <circle id="qm-final-circle" cx="535" cy="450" r="77" pathLength="1" class="qm-intro-stroke" stroke-width="14"/>
        <path id="qm-final-arc" pathLength="1" class="qm-intro-stroke" stroke-width="9"
          d="M427 478 c25-74 98-115 170-78 c20 10 37 26 48 46"/>
        <circle id="qm-final-target" cx="535" cy="450" r="18" fill="currentColor"/>
      </g>
      <text id="qm-wordmark" x="680" y="478" clip-path="url(#qm-wordmark-clip)" class="qm-intro-wordmark">
        Quant<tspan class="master">Master</tspan>
      </text>
    </svg>`;
  document.body.appendChild(overlay);

  const byId = id => overlay.querySelector('#' + id);
  const elements = {
    monoSun: byId('qm-mono-sun'),
    monoSquare: byId('qm-mono-square'),
    ground: byId('qm-ground'),
    horizon: byId('qm-horizon'),
    horizonRaw: byId('qm-horizon-raw'),
    horizonEcho: byId('qm-horizon-echo'),
    horizonStill: byId('qm-horizon-still'),
    inkTip: byId('qm-ink-tip'),
    eye: byId('qm-eye'),
    iris: byId('qm-iris'),
    irisRing: byId('qm-iris-ring'),
    irisFine: byId('qm-iris-fine'),
    pupil: byId('qm-pupil'),
    factor: byId('qm-factor'),
    factorWhite: byId('qm-factor-white'),
    factorBlue: byId('qm-factor-blue'),
    factorPulse: byId('qm-factor-pulse'),
    factorTrail: byId('qm-factor-trail'),
    blueSun: byId('qm-blue-sun'),
    bloom: byId('qm-bloom'),
    blueSquareFlash: byId('qm-blue-square-flash'),
    finalMark: byId('qm-final-mark'),
    finalCircle: byId('qm-final-circle'),
    finalArc: byId('qm-final-arc'),
    finalTarget: byId('qm-final-target'),
    wordmark: byId('qm-wordmark'),
    wordmarkClip: byId('qm-wordmark-clip-rect'),
  };

  [elements.horizonRaw, elements.horizonEcho, elements.horizonStill,
    elements.irisRing, elements.irisFine, elements.factorPulse,
    elements.factorTrail, elements.finalCircle, elements.finalArc].forEach(element => {
    element.style.strokeDasharray = '1';
    element.style.strokeDashoffset = '1';
  });

  const rawLength = elements.horizonRaw.getTotalLength();
  const setOpacity = (element, value) => { element.style.opacity = String(clamp(value)); };
  const setTransform = (element, value) => { element.setAttribute('transform', value); };

  function gazePosition(time) {
    const moves = [
      {start: 3.32, end: 3.50, from: [0, 0], to: [-72, -18]},
      {start: 3.84, end: 4.02, from: [-72, -18], to: [56, 25]},
      {start: 4.27, end: 4.43, from: [56, 25], to: [-42, 34]},
      {start: 4.67, end: 4.82, from: [-42, 34], to: [13, -9]},
      {start: 5.04, end: 5.20, from: [13, -9], to: [82, -49]},
    ];
    let position = [0, 0];
    for (const move of moves) {
      if (time < move.start) return position;
      if (time <= move.end) {
        const progress = phase(time, move.start, move.end, expoOut);
        return [mix(move.from[0], move.to[0], progress), mix(move.from[1], move.to[1], progress)];
      }
      position = move.to;
    }
    return position;
  }

  function quadraticPoint(p0, p1, p2, progress) {
    const inverse = 1 - progress;
    return [
      inverse * inverse * p0[0] + 2 * inverse * progress * p1[0] + progress * progress * p2[0],
      inverse * inverse * p0[1] + 2 * inverse * progress * p1[1] + progress * progress * p2[1],
    ];
  }

  function render(rawTime) {
    const time = Math.max(0, Math.min(DURATION, rawTime));

    const handDraw = phase(time, .46, 1.62, expoOut);
    const horizonSettle = phase(time, 1.42, 2.12, expoOut);
    const eyeOpen = phase(time, 2.40, 3.34, expoOut);
    const eyeClose = phase(time, 5.86, 6.66, easeInOut);
    const irisReveal = phase(time, 2.72, 3.38, expoOut);
    const factorReveal = phase(time, 4.72, 5.08, expoOut);
    const factorColor = phase(time, 5.52, 5.66, expoOut);
    const factorDepart = phase(time, 5.82, 5.94, easeInCubic);
    const travel = phase(time, 5.82, 6.92, easeInOut);
    const sunArrival = phase(time, 6.56, 7.06, expoOut);
    const finalMorph = phase(time, 7.30, 8.02, easeInOut);
    const finalDraw = phase(time, 7.40, 8.04, expoOut);

    const rawFade = 1 - phase(time, 1.54, 2.15, smooth);
    elements.horizonRaw.style.strokeDashoffset = String(1 - handDraw);
    elements.horizonEcho.style.strokeDashoffset = String(1 - clamp(handDraw * 1.06));
    setOpacity(elements.horizonRaw, handDraw * rawFade);
    setOpacity(elements.horizonEcho, handDraw * rawFade * .68);
    elements.horizonStill.style.strokeDashoffset = String(1 - horizonSettle);
    const horizonReturn = phase(time, 5.88, 6.36, expoOut);
    const horizonFinalFade = 1 - phase(time, 7.30, 7.90, smooth);
    setOpacity(elements.horizonStill, Math.max(horizonSettle * (1 - eyeOpen), horizonReturn) * horizonFinalFade);

    const tipPoint = elements.horizonRaw.getPointAtLength(rawLength * handDraw);
    setTransform(elements.inkTip, `translate(${tipPoint.x.toFixed(2)} ${tipPoint.y.toFixed(2)})`);
    setOpacity(elements.inkTip, handDraw > 0 && handDraw < .995 ? rawFade : 0);

    const rise = phase(time, 1.30, 2.42, expoOut);
    const sunToPupil = phase(time, 2.48, 3.15, easeInOut);
    const monoX = 800;
    const monoY = mix(mix(560, 397, rise), 470, sunToPupil);
    const monoScale = mix(mix(.76, 1, rise), .42, sunToPupil);
    setTransform(elements.monoSun, `translate(${monoX} ${monoY.toFixed(2)}) scale(${monoScale.toFixed(4)})`);
    elements.monoSquare.setAttribute('rx', String(mix(0, 42, sunToPupil)));
    setOpacity(elements.monoSun, phase(time, 1.52, 1.90, expoOut) * (1 - phase(time, 2.96, 3.24, smooth)));
    setOpacity(elements.ground, 1 - phase(time, 2.34, 2.74, smooth));

    const eyeScaleY = eyeOpen * mix(1, .055, eyeClose);
    setTransform(elements.eye, `translate(800 470) scale(1 ${eyeScaleY.toFixed(4)}) translate(-800 -470)`);
    setOpacity(elements.eye, eyeOpen * (1 - phase(time, 5.98, 6.56, smooth)) * (1 - phase(time, 6.70, 7.42, smooth)));
    elements.irisRing.style.strokeDashoffset = String(1 - irisReveal);
    elements.irisFine.style.strokeDashoffset = String(1 - phase(time, 2.90, 3.56, expoOut));
    setOpacity(elements.iris, irisReveal * (1 - phase(time, 5.72, 6.45, smooth)));

    const gaze = gazePosition(time);
    const pupilScale = mix(1, .78, factorColor) * mix(1, .76, eyeClose);
    setTransform(elements.pupil, `translate(${(800 + gaze[0]).toFixed(2)} ${(470 + gaze[1]).toFixed(2)}) scale(${pupilScale.toFixed(4)})`);
    setOpacity(elements.pupil, phase(time, 2.92, 3.24, expoOut) * (1 - phase(time, 5.96, 6.54, smooth)));

    const factorScale = mix(.12, 1, factorReveal);
    setTransform(elements.factor, `translate(1110 330) scale(${factorScale.toFixed(4)})`);
    setOpacity(elements.factor, factorReveal * (1 - factorDepart));
    setOpacity(elements.factorWhite, 1 - factorColor);
    setOpacity(elements.factorBlue, factorColor);
    elements.factorPulse.style.strokeDashoffset = String(1 - phase(time, 5.48, 5.72, expoOut));
    setOpacity(elements.factorPulse, factorColor * (1 - phase(time, 5.72, 5.96, smooth)));

    elements.factorTrail.style.strokeDashoffset = String(1 - travel);
    setOpacity(elements.factorTrail, travel * (1 - phase(time, 6.54, 6.96, smooth)) * .75);

    const travelPoint = quadraticPoint([1110, 330], [1285, 430], [800, 470], travel);
    const sharedX = mix(travelPoint[0], 621.5, finalMorph);
    const sharedY = mix(travelPoint[1], 392.5, finalMorph);
    const travelScale = mix(.39, 1, travel);
    const sharedScale = mix(travelScale, .3611, finalMorph);
    setTransform(elements.blueSun,
      `translate(${sharedX.toFixed(2)} ${sharedY.toFixed(2)}) scale(${sharedScale.toFixed(4)})`);
    setOpacity(elements.blueSun, factorDepart);

    const bloomOut = 1 - phase(time, 7.30, 7.86, smooth);
    const bloomOpacity = sunArrival * bloomOut;
    setOpacity(elements.bloom, bloomOpacity);
    const bloomScale = mix(.48, 1.06, sunArrival) * mix(1, 1.24, phase(time, 6.90, 7.24, easeInOut));
    setTransform(elements.bloom, `scale(${bloomScale.toFixed(4)}) rotate(${mix(-9, 0, sunArrival).toFixed(2)})`);
    const flashIn = phase(time, 6.76, 6.88, expoOut);
    const flashOut = phase(time, 6.88, 7.10, smooth);
    setOpacity(elements.blueSquareFlash, flashIn * (1 - flashOut));

    const finalArcDraw = phase(time, 7.36, 7.88, expoOut);
    elements.finalCircle.style.strokeDasharray = finalDraw > .998 ? 'none' : '1';
    elements.finalCircle.style.strokeDashoffset = finalDraw > .998 ? '0' : String(1 - finalDraw);
    elements.finalArc.style.strokeDasharray = finalArcDraw > .998 ? 'none' : '1';
    elements.finalArc.style.strokeDashoffset = finalArcDraw > .998 ? '0' : String(1 - finalArcDraw);
    setOpacity(elements.finalTarget, phase(time, 7.70, 7.94, expoOut));
    setOpacity(elements.finalMark, finalMorph);
    const markSettle = mix(.965, 1, finalDraw);
    setTransform(elements.finalMark, `translate(535 450) scale(${markSettle.toFixed(4)}) translate(-535 -450)`);

    const wordReveal = phase(time, 7.62, 8.16, expoOut);
    elements.wordmarkClip.setAttribute('width', String(520 * wordReveal));
    setOpacity(elements.wordmark, phase(time, 7.56, 7.72, expoOut));
    setTransform(elements.wordmark, `translate(${mix(18, 0, wordReveal).toFixed(2)} 0)`);

    return time;
  }

  let animationFrame = null;
  let lastTick = null;
  let currentTime = 0;
  let playing = false;
  let loopTimer = null;

  function cancelPlayback() {
    playing = false;
    if (animationFrame !== null) cancelAnimationFrame(animationFrame);
    if (loopTimer !== null) clearTimeout(loopTimer);
    animationFrame = null;
    loopTimer = null;
    lastTick = null;
  }

  function hideAfterExit() {
    if (config.mode === 'preview' || recording) return;
    overlay.classList.add('is-exiting');
    window.setTimeout(() => {
      overlay.hidden = true;
      overlay.classList.remove('is-exiting');
      document.body.classList.remove('qm-intro-active');
    }, reducedMotion ? 100 : 380);
  }

  function finishPlayback() {
    playing = false;
    animationFrame = null;
    if (recording) {
      currentTime = DURATION - .001;
      render(currentTime);
      return;
    }
    if (config.mode === 'preview' && config.loop) {
      loopTimer = window.setTimeout(() => play({markSeen: false}), 1100);
      return;
    }
    hideAfterExit();
  }

  function tick(now) {
    if (!playing) return;
    if (lastTick === null) {
      lastTick = now;
      window.__ready = true;
      render(0);
      animationFrame = requestAnimationFrame(tick);
      return;
    }
    currentTime += (now - lastTick) / 1000;
    lastTick = now;
    if (currentTime >= DURATION) {
      currentTime = DURATION;
      render(currentTime);
      finishPlayback();
      return;
    }
    render(currentTime);
    animationFrame = requestAnimationFrame(tick);
  }

  function play(options = {}) {
    cancelPlayback();
    overlay.hidden = false;
    overlay.classList.remove('is-exiting');
    document.body.classList.add('qm-intro-active');
    currentTime = 0;
    render(0);
    if (options.markSeen !== false && hasSessionStorage) sessionStorage.setItem(SESSION_KEY, 'seen');

    if (reducedMotion) {
      render(DURATION);
      window.__ready = true;
      if (config.mode !== 'preview') window.setTimeout(hideAfterExit, 520);
      return;
    }

    playing = true;
    lastTick = null;
    animationFrame = requestAnimationFrame(tick);
  }

  function skip() {
    if (overlay.hidden) return;
    cancelPlayback();
    currentTime = DURATION;
    render(currentTime);
    hideAfterExit();
  }

  function attachReplayControl() {
    const replay = document.getElementById('brand-replay');
    if (!replay || replay.dataset.brandIntroBound === 'true') return;
    replay.dataset.brandIntroBound = 'true';
    replay.addEventListener('click', () => play({markSeen: true}));
  }

  window.QuantMasterBrandIntro = {play, replay: play, skip, render, duration: DURATION};
  window.__seek = time => {
    cancelPlayback();
    overlay.hidden = false;
    overlay.classList.remove('is-exiting');
    currentTime = Math.max(0, Math.min(DURATION, Number(time) || 0));
    render(currentTime);
  };

  document.addEventListener('keydown', event => {
    if (event.key === 'Escape' && config.mode !== 'preview') skip();
    if (config.mode === 'preview' && event.key.toLowerCase() === 'r') play({markSeen: false});
  });

  const domReady = document.readyState === 'loading'
    ? new Promise(resolve => document.addEventListener('DOMContentLoaded', resolve, {once: true}))
    : Promise.resolve();
  const fontsReady = document.fonts?.ready || Promise.resolve();

  domReady.then(attachReplayControl);
  if (shouldAutoplay) {
    overlay.hidden = false;
    render(0);
    Promise.all([domReady, fontsReady]).then(() => play({markSeen: true}));
  } else {
    window.__ready = true;
  }
})();
