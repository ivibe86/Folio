<script>
    export let height = 340;
    export let eyebrow = 'Syncing money flow';
    export let mode = 'empty';
    export let showCorner = true;

    $: isStatusMode = mode === 'empty' || mode === 'sync';
</script>

<div
    class="sankey-loading-stage"
    class:sankey-loading-stage--overlay={mode === 'overlay'}
    class:sankey-loading-stage--underlay={mode === 'underlay'}
    class:sankey-loading-stage--sync={mode === 'sync'}
    style={isStatusMode ? `height: ${height}px;` : undefined}
    role={isStatusMode ? 'status' : undefined}
    aria-live={isStatusMode ? 'polite' : undefined}
    aria-label={isStatusMode ? eyebrow : undefined}
    aria-hidden={!isStatusMode}
>
    {#if showCorner}
        <div class="sankey-loading-stage__corner">
            <span class="sankey-loading-stage__pulse"></span>
            {eyebrow}
        </div>
    {/if}
    <div class="sankey-loading-stage__anchors" aria-hidden="true">
        <div class="sankey-loading-stage__anchor sankey-loading-stage__anchor--source"></div>
        <div class="sankey-loading-stage__anchor sankey-loading-stage__anchor--hub"></div>
        <div class="sankey-loading-stage__anchor sankey-loading-stage__anchor--dest"></div>
    </div>
    <div class="sankey-loading-stage__veil"></div>
    <div class="sankey-loading-stage__nebula sankey-loading-stage__nebula--left"></div>
    <div class="sankey-loading-stage__nebula sankey-loading-stage__nebula--center"></div>
    <div class="sankey-loading-stage__nebula sankey-loading-stage__nebula--right"></div>
    <div class="sankey-loading-stage__stream sankey-loading-stage__stream--upper"></div>
    <div class="sankey-loading-stage__stream sankey-loading-stage__stream--lower"></div>
    <div class="sankey-loading-stage__stream sankey-loading-stage__stream--accent"></div>
    <div class="sankey-loading-stage__core">
        <div class="sankey-loading-stage__core-cloud sankey-loading-stage__core-cloud--a"></div>
        <div class="sankey-loading-stage__core-cloud sankey-loading-stage__core-cloud--b"></div>
        <div class="sankey-loading-stage__core-cloud sankey-loading-stage__core-cloud--c"></div>
    </div>
</div>

<style>
    .sankey-loading-stage {
        position: relative;
        width: 100%;
        min-height: 340px;
        overflow: hidden;
        border-radius: 16px;
        isolation: isolate;
        pointer-events: none;
    }

    .sankey-loading-stage--overlay {
        position: absolute;
        inset: -3% 0 2% 0;
        min-height: 0;
        height: auto;
        border-radius: inherit;
        z-index: 3;
        mask-image: linear-gradient(180deg, rgba(0, 0, 0, 1) 0%, rgba(0, 0, 0, 1) 82%, rgba(0, 0, 0, 0.78) 92%, transparent 100%);
        -webkit-mask-image: linear-gradient(180deg, rgba(0, 0, 0, 1) 0%, rgba(0, 0, 0, 1) 82%, rgba(0, 0, 0, 0.78) 92%, transparent 100%);
    }

    .sankey-loading-stage--underlay {
        position: absolute;
        inset: 0;
        min-height: 0;
        height: auto;
        border-radius: inherit;
        opacity: 0.62;
    }

    .sankey-loading-stage--sync {
        min-height: 340px;
        background:
            radial-gradient(circle at 50% 48%, rgba(56, 189, 248, 0.08) 0%, transparent 28%),
            linear-gradient(180deg, rgba(4, 8, 20, 0.78) 0%, rgba(3, 7, 18, 0.9) 58%, rgba(2, 6, 16, 0.96) 100%);
        box-shadow:
            inset 0 1px 0 rgba(255, 255, 255, 0.03),
            inset 0 0 100px rgba(5, 10, 26, 0.62);
    }

    .sankey-loading-stage__corner {
        position: absolute;
        top: 0.95rem;
        right: 0.95rem;
        z-index: 4;
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        padding: 0.48rem 0.74rem;
        border-radius: 999px;
        background: rgba(7, 11, 22, 0.38);
        border: 1px solid rgba(125, 211, 252, 0.14);
        box-shadow:
            0 10px 24px rgba(2, 6, 23, 0.18),
            inset 0 1px 0 rgba(255, 255, 255, 0.03);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        color: rgba(207, 250, 254, 0.86);
        font-size: 0.625rem;
        font-weight: 700;
        letter-spacing: 0.16em;
        text-transform: uppercase;
    }

    .sankey-loading-stage__veil,
    .sankey-loading-stage__anchors,
    .sankey-loading-stage__anchor,
    .sankey-loading-stage__nebula,
    .sankey-loading-stage__stream,
    .sankey-loading-stage__core {
        position: absolute;
        pointer-events: none;
    }

    .sankey-loading-stage__veil {
        inset: 0;
        background:
            radial-gradient(circle at 50% 46%, rgba(56, 189, 248, 0.08) 0%, rgba(56, 189, 248, 0.02) 24%, transparent 54%),
            radial-gradient(circle at 50% 60%, rgba(2, 6, 23, 0) 0%, rgba(2, 6, 23, 0.12) 45%, rgba(2, 6, 23, 0.38) 100%),
            linear-gradient(180deg, rgba(255, 255, 255, 0.025) 0%, rgba(15, 23, 42, 0.04) 22%, rgba(15, 23, 42, 0.18) 100%);
        z-index: 0;
    }

    .sankey-loading-stage--overlay .sankey-loading-stage__veil {
        background:
            radial-gradient(circle at 48% 38%, rgba(103, 232, 249, 0.24) 0%, rgba(56, 189, 248, 0.12) 28%, transparent 56%),
            radial-gradient(circle at 26% 34%, rgba(45, 212, 191, 0.12) 0%, transparent 42%),
            linear-gradient(90deg, rgba(2, 6, 16, 0.54) 0%, rgba(3, 8, 20, 0.38) 34%, rgba(5, 10, 24, 0.52) 100%),
            linear-gradient(180deg, rgba(2, 6, 18, 0.34) 0%, rgba(2, 6, 23, 0.48) 52%, rgba(2, 6, 23, 0.3) 100%);
    }

    .sankey-loading-stage--underlay .sankey-loading-stage__veil {
        background:
            linear-gradient(90deg, rgba(45, 212, 191, 0.03) 0%, rgba(56, 189, 248, 0.02) 34%, rgba(129, 140, 248, 0.035) 72%, rgba(192, 132, 252, 0.03) 100%),
            linear-gradient(180deg, rgba(255, 255, 255, 0.012) 0%, rgba(15, 23, 42, 0.02) 22%, rgba(15, 23, 42, 0.09) 100%);
    }

    .sankey-loading-stage--sync .sankey-loading-stage__veil {
        background:
            radial-gradient(circle at 20% 44%, rgba(45, 212, 191, 0.10) 0%, transparent 24%),
            radial-gradient(circle at 54% 42%, rgba(59, 130, 246, 0.18) 0%, transparent 24%),
            radial-gradient(circle at 108% 48%, rgba(129, 140, 248, 0.30) 0%, rgba(129, 140, 248, 0.12) 16%, transparent 38%),
            linear-gradient(90deg, rgba(4, 8, 20, 0.34) 0%, rgba(4, 9, 22, 0.12) 26%, rgba(4, 9, 22, 0.04) 54%, rgba(4, 9, 22, 0.10) 76%, rgba(4, 8, 20, 0.30) 100%),
            radial-gradient(circle at 50% 70%, rgba(2, 6, 23, 0) 0%, rgba(2, 6, 23, 0.18) 46%, rgba(2, 6, 23, 0.48) 100%);
    }

    .sankey-loading-stage__anchors {
        inset: 0;
        z-index: 0;
        opacity: 0;
    }

    .sankey-loading-stage--sync .sankey-loading-stage__anchors {
        opacity: 1;
    }

    .sankey-loading-stage__anchor {
        top: 12%;
        bottom: 14%;
        border-radius: 999px;
        border: 1px solid rgba(186, 230, 253, 0.08);
        background:
            linear-gradient(180deg, rgba(186, 230, 253, 0.10) 0%, rgba(148, 163, 184, 0.06) 100%);
        box-shadow:
            inset 0 0 20px rgba(255, 255, 255, 0.02),
            0 0 32px rgba(56, 189, 248, 0.05);
    }

    .sankey-loading-stage__anchor--source {
        left: 2.2%;
        width: 18px;
        opacity: 0.4;
    }

    .sankey-loading-stage__anchor--hub {
        left: 49%;
        top: 14%;
        bottom: 16%;
        width: 14px;
        opacity: 0.46;
    }

    .sankey-loading-stage__anchor--dest {
        right: 1.8%;
        width: 14px;
        opacity: 0.22;
        box-shadow:
            inset 0 0 22px rgba(99, 102, 241, 0.06),
            0 0 54px rgba(129, 140, 248, 0.08);
    }

    .sankey-loading-stage__nebula {
        border-radius: 999px;
        mix-blend-mode: screen;
        filter: blur(30px);
        opacity: 0.72;
        will-change: transform, opacity;
        z-index: 1;
    }

    .sankey-loading-stage--overlay .sankey-loading-stage__nebula {
        filter: blur(22px);
        opacity: 0.98;
    }

    .sankey-loading-stage--underlay .sankey-loading-stage__nebula {
        filter: blur(26px);
        opacity: 0.58;
    }

    .sankey-loading-stage--underlay .sankey-loading-stage__nebula--center {
        display: none;
    }

    .sankey-loading-stage--sync .sankey-loading-stage__nebula {
        filter: blur(28px);
        opacity: 1;
    }

    .sankey-loading-stage--overlay .sankey-loading-stage__nebula--left {
        top: 8%;
        left: -5%;
        width: 50%;
        height: 42%;
    }

    .sankey-loading-stage--overlay .sankey-loading-stage__nebula--center {
        top: 11%;
        left: 22%;
        width: 56%;
        height: 48%;
    }

    .sankey-loading-stage--overlay .sankey-loading-stage__nebula--right {
        top: 9%;
        right: -4%;
        width: 42%;
        height: 44%;
    }

    .sankey-loading-stage__nebula--left {
        left: -3%;
        top: 16%;
        width: 42%;
        height: 38%;
        background:
            radial-gradient(circle at 30% 50%, rgba(45, 212, 191, 0.42) 0%, rgba(45, 212, 191, 0.18) 36%, transparent 76%);
        animation: sankeyNebulaFloatLeft 10s ease-in-out infinite alternate;
    }

    .sankey-loading-stage--sync .sankey-loading-stage__nebula--left {
        left: -2%;
        top: 18%;
        width: 50%;
        height: 46%;
        background:
            radial-gradient(circle at 26% 48%, rgba(45, 212, 191, 0.84) 0%, rgba(45, 212, 191, 0.46) 28%, rgba(14, 165, 233, 0.18) 52%, transparent 76%);
        animation: sankeySyncNebulaLeft 15s ease-in-out infinite alternate;
    }

    .sankey-loading-stage__nebula--center {
        left: 27%;
        top: 21%;
        width: 46%;
        height: 42%;
        background:
            radial-gradient(circle at 50% 50%, rgba(103, 232, 249, 0.46) 0%, rgba(56, 189, 248, 0.20) 34%, transparent 74%);
        animation: sankeyNebulaPulse 7.2s ease-in-out infinite;
    }

    .sankey-loading-stage--sync .sankey-loading-stage__nebula--center {
        left: 20%;
        top: 18%;
        width: 60%;
        height: 48%;
        background:
            radial-gradient(circle at 46% 52%, rgba(125, 211, 252, 0.98) 0%, rgba(56, 189, 248, 0.62) 24%, rgba(59, 130, 246, 0.34) 44%, rgba(99, 102, 241, 0.18) 58%, transparent 78%);
        animation: sankeySyncNebulaCenter 13.5s ease-in-out infinite alternate;
    }

    .sankey-loading-stage__nebula--right {
        right: -2%;
        top: 12%;
        width: 36%;
        height: 44%;
        background:
            radial-gradient(circle at 58% 44%, rgba(129, 140, 248, 0.32) 0%, rgba(129, 140, 248, 0.14) 36%, transparent 76%);
        animation: sankeyNebulaFloatRight 11.5s ease-in-out infinite alternate;
    }

    .sankey-loading-stage--sync .sankey-loading-stage__nebula--right {
        right: -4%;
        top: 12%;
        width: 46%;
        height: 50%;
        background:
            radial-gradient(circle at 72% 48%, rgba(129, 140, 248, 0.78) 0%, rgba(99, 102, 241, 0.44) 24%, rgba(192, 132, 252, 0.20) 44%, transparent 76%);
        animation: sankeySyncNebulaRight 14.5s ease-in-out infinite alternate;
    }

    .sankey-loading-stage__stream {
        left: -20%;
        width: 72%;
        border-radius: 999px;
        mix-blend-mode: screen;
        filter: blur(18px);
        opacity: 0.74;
        will-change: transform, opacity;
        z-index: 2;
    }

    .sankey-loading-stage--overlay .sankey-loading-stage__stream {
        left: -18%;
        width: 88%;
        filter: blur(12px);
        opacity: 1;
    }

    .sankey-loading-stage--underlay .sankey-loading-stage__stream {
        filter: blur(16px);
        opacity: 0.54;
    }

    .sankey-loading-stage--sync .sankey-loading-stage__stream {
        left: -16%;
        width: 88%;
        filter: blur(20px);
        opacity: 0.92;
    }

    .sankey-loading-stage--overlay .sankey-loading-stage__stream--upper {
        top: 10%;
        height: 22%;
    }

    .sankey-loading-stage--overlay .sankey-loading-stage__stream--lower {
        top: 28%;
        height: 24%;
    }

    .sankey-loading-stage__stream--upper {
        top: 18%;
        height: 22%;
        background:
            linear-gradient(
                90deg,
                transparent 0%,
                rgba(45, 212, 191, 0) 12%,
                rgba(45, 212, 191, 0.26) 28%,
                rgba(103, 232, 249, 0.40) 50%,
                rgba(99, 102, 241, 0.18) 74%,
                transparent 100%
            );
        transform: rotate(-8deg);
        animation: sankeyMistSweepUpper 9.8s ease-in-out infinite;
    }

    .sankey-loading-stage--sync .sankey-loading-stage__stream--upper {
        top: 17%;
        height: 26%;
        background:
            radial-gradient(ellipse at 28% 54%, rgba(45, 212, 191, 0.64) 0%, rgba(45, 212, 191, 0.32) 22%, transparent 62%),
            radial-gradient(ellipse at 54% 48%, rgba(56, 189, 248, 0.86) 0%, rgba(56, 189, 248, 0.38) 26%, transparent 64%),
            radial-gradient(ellipse at 74% 46%, rgba(99, 102, 241, 0.34) 0%, transparent 56%);
        animation: sankeySyncStreamUpper 16s ease-in-out infinite;
    }

    .sankey-loading-stage__stream--lower {
        top: 48%;
        height: 24%;
        background:
            linear-gradient(
                90deg,
                transparent 0%,
                rgba(56, 189, 248, 0) 14%,
                rgba(56, 189, 248, 0.18) 30%,
                rgba(125, 211, 252, 0.36) 50%,
                rgba(45, 212, 191, 0.16) 76%,
                transparent 100%
            );
        transform: rotate(6deg);
        animation: sankeyMistSweepLower 11.2s ease-in-out infinite;
    }

    .sankey-loading-stage--sync .sankey-loading-stage__stream--lower {
        top: 36%;
        height: 28%;
        background:
            radial-gradient(ellipse at 30% 52%, rgba(96, 165, 250, 0.72) 0%, rgba(96, 165, 250, 0.34) 24%, transparent 62%),
            radial-gradient(ellipse at 52% 56%, rgba(125, 211, 252, 0.78) 0%, rgba(125, 211, 252, 0.36) 22%, transparent 60%),
            radial-gradient(ellipse at 74% 54%, rgba(129, 140, 248, 0.42) 0%, rgba(192, 132, 252, 0.20) 24%, transparent 64%);
        animation: sankeySyncStreamLower 17.5s ease-in-out infinite;
    }

    .sankey-loading-stage__stream--accent {
        top: 33%;
        left: -12%;
        width: 52%;
        height: 18%;
        background:
            linear-gradient(
                90deg,
                transparent 0%,
                rgba(186, 230, 253, 0) 18%,
                rgba(186, 230, 253, 0.24) 46%,
                rgba(186, 230, 253, 0.08) 72%,
                transparent 100%
            );
        filter: blur(12px);
        opacity: 0.66;
        transform: rotate(-2deg);
        animation: sankeyMistSweepAccent 6.4s ease-in-out infinite;
    }

    .sankey-loading-stage--sync .sankey-loading-stage__stream--accent {
        top: 26%;
        left: -10%;
        width: 80%;
        height: 24%;
        background:
            radial-gradient(ellipse at 40% 50%, rgba(191, 219, 254, 0.62) 0%, rgba(59, 130, 246, 0.28) 26%, transparent 60%),
            radial-gradient(ellipse at 66% 56%, rgba(129, 140, 248, 0.40) 0%, rgba(192, 132, 252, 0.16) 24%, transparent 62%);
        filter: blur(18px);
        opacity: 0.78;
        animation: sankeySyncStreamAccent 15.5s ease-in-out infinite;
    }

    .sankey-loading-stage--overlay .sankey-loading-stage__stream--accent {
        top: 18%;
        width: 70%;
        opacity: 0.94;
    }

    .sankey-loading-stage__core {
        top: 46%;
        left: 50%;
        width: 320px;
        height: 190px;
        transform: translate(-50%, -50%);
        filter: blur(10px);
        opacity: 0.88;
        z-index: 2;
        animation: sankeyCoreDrift 10s ease-in-out infinite alternate;
    }

    .sankey-loading-stage--overlay .sankey-loading-stage__core {
        top: 34%;
        width: 460px;
        height: 210px;
        filter: blur(7px);
        opacity: 1;
    }

    .sankey-loading-stage--overlay .sankey-loading-stage__core-cloud--a {
        opacity: 0.72;
    }

    .sankey-loading-stage--overlay .sankey-loading-stage__core-cloud--b {
        opacity: 0.62;
    }

    .sankey-loading-stage--overlay .sankey-loading-stage__core-cloud--c {
        opacity: 0.42;
    }

    .sankey-loading-stage--underlay .sankey-loading-stage__core {
        display: none;
    }

    .sankey-loading-stage--sync .sankey-loading-stage__core {
        top: 44%;
        left: 56%;
        width: 460px;
        height: 260px;
        filter: blur(14px);
        opacity: 0.88;
        animation: sankeySyncCoreDrift 16s ease-in-out infinite alternate;
    }

    .sankey-loading-stage__core-cloud {
        position: absolute;
        border-radius: 999px;
        mix-blend-mode: screen;
        filter: blur(22px);
        will-change: transform, opacity;
    }

    .sankey-loading-stage__core-cloud--a {
        inset: 18% auto auto 10%;
        width: 52%;
        height: 54%;
        background:
            radial-gradient(circle at 38% 48%, rgba(186, 230, 253, 0.22) 0%, rgba(125, 211, 252, 0.12) 34%, transparent 76%);
        opacity: 0.5;
        animation: sankeyCoreCloudA 7.8s ease-in-out infinite alternate;
    }

    .sankey-loading-stage--sync .sankey-loading-stage__core-cloud--a {
        background:
            radial-gradient(circle at 42% 50%, rgba(125, 211, 252, 0.36) 0%, rgba(59, 130, 246, 0.20) 30%, transparent 78%);
        opacity: 0.62;
        animation: sankeySyncCoreCloudA 15s ease-in-out infinite alternate;
    }

    .sankey-loading-stage__core-cloud--b {
        inset: 28% 8% auto auto;
        width: 42%;
        height: 48%;
        background:
            radial-gradient(circle at 54% 46%, rgba(103, 232, 249, 0.18) 0%, rgba(56, 189, 248, 0.08) 36%, transparent 78%);
        opacity: 0.44;
        animation: sankeyCoreCloudB 9.2s ease-in-out infinite alternate;
    }

    .sankey-loading-stage--sync .sankey-loading-stage__core-cloud--b {
        background:
            radial-gradient(circle at 58% 48%, rgba(96, 165, 250, 0.28) 0%, rgba(37, 99, 235, 0.16) 32%, transparent 78%);
        opacity: 0.56;
        animation: sankeySyncCoreCloudB 16.5s ease-in-out infinite alternate;
    }

    .sankey-loading-stage__core-cloud--c {
        inset: 34% auto auto 30%;
        width: 34%;
        height: 34%;
        background:
            radial-gradient(circle at 48% 50%, rgba(255, 255, 255, 0.08) 0%, rgba(186, 230, 253, 0.05) 32%, transparent 74%);
        opacity: 0.28;
        animation: sankeyCoreCloudC 6.8s ease-in-out infinite alternate;
    }

    .sankey-loading-stage--sync .sankey-loading-stage__core-cloud--c {
        background:
            radial-gradient(circle at 50% 52%, rgba(129, 140, 248, 0.22) 0%, rgba(192, 132, 252, 0.10) 28%, transparent 74%);
        opacity: 0.44;
        animation: sankeySyncCoreCloudC 14s ease-in-out infinite alternate;
    }

    .sankey-loading-stage__pulse {
        width: 8px;
        height: 8px;
        border-radius: 999px;
        background: rgba(103, 232, 249, 0.96);
        box-shadow:
            0 0 0 0 rgba(103, 232, 249, 0.45),
            0 0 12px rgba(103, 232, 249, 0.5);
        animation: sankeyPulseDot 1.8s ease-out infinite;
    }

    :global(:root:not(.dark)) .sankey-loading-stage__corner {
        background: rgba(8, 13, 24, 0.32);
        border-color: rgba(103, 232, 249, 0.12);
    }

    @keyframes sankeyNebulaFloatLeft {
        0% {
            transform: translate3d(-4%, 2%, 0) scale(0.98);
            opacity: 0.58;
        }
        100% {
            transform: translate3d(10%, -4%, 0) scale(1.08);
            opacity: 0.84;
        }
    }

    @keyframes sankeySyncNebulaLeft {
        0% {
            transform: translate3d(-6%, 4%, 0) scale(0.98) rotate(-5deg);
        }
        35% {
            transform: translate3d(-1%, -1%, 0) scale(1.04) rotate(-1deg);
        }
        68% {
            transform: translate3d(5%, 2%, 0) scale(1.1) rotate(3deg);
        }
        100% {
            transform: translate3d(10%, -6%, 0) scale(1.14) rotate(4deg);
        }
    }

    @keyframes sankeyNebulaPulse {
        0%, 100% {
            transform: translate3d(0, 0, 0) scale(0.96);
            opacity: 0.6;
        }
        50% {
            transform: translate3d(0, -2%, 0) scale(1.06);
            opacity: 0.9;
        }
    }

    @keyframes sankeySyncNebulaCenter {
        0% {
            transform: translate3d(-2%, 2%, 0) scale(0.94) rotate(-3deg);
        }
        30% {
            transform: translate3d(1%, -1%, 0) scale(1) rotate(-1deg);
        }
        58% {
            transform: translate3d(4%, 2%, 0) scale(1.06) rotate(2deg);
        }
        82% {
            transform: translate3d(6%, -2%, 0) scale(1.1) rotate(4deg);
        }
        100% {
            transform: translate3d(8%, -3%, 0) scale(1.1) rotate(3deg);
        }
    }

    @keyframes sankeyNebulaFloatRight {
        0% {
            transform: translate3d(3%, -1%, 0) scale(0.98);
            opacity: 0.5;
        }
        100% {
            transform: translate3d(-8%, 4%, 0) scale(1.08);
            opacity: 0.72;
        }
    }

    @keyframes sankeySyncNebulaRight {
        0% {
            transform: translate3d(2%, -2%, 0) scale(0.98) rotate(3deg);
        }
        34% {
            transform: translate3d(-2%, 1%, 0) scale(1.04) rotate(1deg);
        }
        72% {
            transform: translate3d(-8%, -1%, 0) scale(1.1) rotate(-3deg);
        }
        100% {
            transform: translate3d(-12%, 6%, 0) scale(1.14) rotate(-5deg);
        }
    }

    @keyframes sankeyMistSweepUpper {
        0% {
            transform: translate3d(0, -3%, 0) rotate(-8deg) scaleX(0.92);
            opacity: 0.18;
        }
        45% {
            opacity: 0.9;
        }
        100% {
            transform: translate3d(138%, 3%, 0) rotate(-5deg) scaleX(1.06);
            opacity: 0.14;
        }
    }

    @keyframes sankeySyncStreamUpper {
        0% {
            transform: translate3d(-8%, 2%, 0) rotate(-7deg) scaleX(0.92) scaleY(0.94);
        }
        26% {
            transform: translate3d(18%, -3%, 0) rotate(-4deg) scaleX(0.98) scaleY(1);
        }
        54% {
            transform: translate3d(46%, 2%, 0) rotate(1deg) scaleX(1.04) scaleY(1.08);
        }
        78% {
            transform: translate3d(76%, -4%, 0) rotate(5deg) scaleX(1.08) scaleY(1.12);
        }
        100% {
            transform: translate3d(108%, -6%, 0) rotate(2deg) scaleX(1.1) scaleY(1.08);
        }
    }

    @keyframes sankeyMistSweepLower {
        0% {
            transform: translate3d(0, 2%, 0) rotate(6deg) scaleX(0.88);
            opacity: 0.16;
        }
        42% {
            opacity: 0.76;
        }
        100% {
            transform: translate3d(130%, -4%, 0) rotate(9deg) scaleX(1.04);
            opacity: 0.12;
        }
    }

    @keyframes sankeySyncStreamLower {
        0% {
            transform: translate3d(-6%, 4%, 0) rotate(7deg) scaleX(0.9) scaleY(0.96);
        }
        24% {
            transform: translate3d(14%, 6%, 0) rotate(4deg) scaleX(0.96) scaleY(1.02);
        }
        52% {
            transform: translate3d(40%, -1%, 0) rotate(0deg) scaleX(1.04) scaleY(1.08);
        }
        76% {
            transform: translate3d(72%, 4%, 0) rotate(-4deg) scaleX(1.08) scaleY(1.12);
        }
        100% {
            transform: translate3d(112%, -4%, 0) rotate(-3deg) scaleX(1.08) scaleY(1.1);
        }
    }

    @keyframes sankeyMistSweepAccent {
        0% {
            transform: translate3d(0, 0, 0) rotate(-2deg) scaleX(0.84);
            opacity: 0;
        }
        28% {
            opacity: 0.8;
        }
        100% {
            transform: translate3d(182%, 1%, 0) rotate(0deg) scaleX(1.08);
            opacity: 0;
        }
    }

    @keyframes sankeySyncStreamAccent {
        0% {
            transform: translate3d(-5%, -1%, 0) rotate(-3deg) scaleX(0.9) scaleY(0.96);
        }
        28% {
            transform: translate3d(12%, -4%, 0) rotate(-1deg) scaleX(0.96) scaleY(1);
        }
        56% {
            transform: translate3d(36%, 3%, 0) rotate(3deg) scaleX(1) scaleY(1.06);
        }
        82% {
            transform: translate3d(70%, -2%, 0) rotate(6deg) scaleX(1.04) scaleY(1.1);
        }
        100% {
            transform: translate3d(96%, 5%, 0) rotate(3deg) scaleX(1.04) scaleY(1.08);
        }
    }

    @keyframes sankeyCoreDrift {
        0% {
            transform: translate(-52%, -49%) scale(0.96);
            opacity: 0.7;
        }
        100% {
            transform: translate(-47%, -52%) scale(1.05);
            opacity: 0.92;
        }
    }

    @keyframes sankeySyncCoreDrift {
        0% {
            transform: translate(-54%, -47%) scale(0.96) rotate(-2deg);
        }
        36% {
            transform: translate(-50%, -50%) scale(1.02) rotate(1deg);
        }
        72% {
            transform: translate(-46%, -48%) scale(1.08) rotate(4deg);
        }
        100% {
            transform: translate(-44%, -52%) scale(1.12) rotate(3deg);
        }
    }

    @keyframes sankeyCoreCloudA {
        0% {
            transform: translate3d(-4%, 2%, 0) scale(0.94);
            opacity: 0.34;
        }
        100% {
            transform: translate3d(6%, -3%, 0) scale(1.08);
            opacity: 0.58;
        }
    }

    @keyframes sankeySyncCoreCloudA {
        0% {
            transform: translate3d(-6%, 3%, 0) scale(0.96) rotate(-3deg);
        }
        42% {
            transform: translate3d(0%, -2%, 0) scale(1.04) rotate(1deg);
        }
        100% {
            transform: translate3d(10%, -5%, 0) scale(1.14) rotate(4deg);
        }
    }

    @keyframes sankeyCoreCloudB {
        0% {
            transform: translate3d(3%, -3%, 0) scale(0.96);
            opacity: 0.3;
        }
        100% {
            transform: translate3d(-5%, 4%, 0) scale(1.06);
            opacity: 0.5;
        }
    }

    @keyframes sankeySyncCoreCloudB {
        0% {
            transform: translate3d(3%, -2%, 0) scale(0.98) rotate(2deg);
        }
        40% {
            transform: translate3d(-2%, 1%, 0) scale(1.04) rotate(-1deg);
        }
        100% {
            transform: translate3d(-9%, 6%, 0) scale(1.12) rotate(-4deg);
        }
    }

    @keyframes sankeyCoreCloudC {
        0% {
            transform: translate3d(-2%, 0, 0) scale(0.9);
            opacity: 0.16;
        }
        100% {
            transform: translate3d(4%, -2%, 0) scale(1.04);
            opacity: 0.32;
        }
    }

    @keyframes sankeySyncCoreCloudC {
        0% {
            transform: translate3d(-3%, 1%, 0) scale(0.94) rotate(-2deg);
        }
        44% {
            transform: translate3d(1%, -2%, 0) scale(1.02) rotate(1deg);
        }
        100% {
            transform: translate3d(7%, -4%, 0) scale(1.1) rotate(3deg);
        }
    }

    @keyframes sankeyPulseDot {
        0% {
            box-shadow:
                0 0 0 0 rgba(103, 232, 249, 0.4),
                0 0 10px rgba(103, 232, 249, 0.42);
        }
        70% {
            box-shadow:
                0 0 0 10px rgba(103, 232, 249, 0),
                0 0 16px rgba(103, 232, 249, 0.5);
        }
        100% {
            box-shadow:
                0 0 0 0 rgba(103, 232, 249, 0),
                0 0 10px rgba(103, 232, 249, 0.36);
        }
    }

    @media (max-width: 768px) {
        .sankey-loading-stage__corner {
            top: 0.75rem;
            right: 0.75rem;
            padding: 0.42rem 0.66rem;
            font-size: 0.56rem;
        }
    }

    @media (prefers-reduced-motion: reduce) {
        .sankey-loading-stage__anchors,
        .sankey-loading-stage__nebula,
        .sankey-loading-stage__stream,
        .sankey-loading-stage__core,
        .sankey-loading-stage__pulse {
            animation: none;
        }
    }
</style>
