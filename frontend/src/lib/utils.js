import { privacyMode } from '$lib/stores.js';
import { get } from 'svelte/store';

const PRIVACY_MASK = '$•••••';

export function formatCurrency(value, decimals = 0) {
    if (get(privacyMode)) return PRIVACY_MASK;
    const num = typeof value === 'string' ? parseFloat(value) : value;
    if (isNaN(num)) return '$0';
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    }).format(num);
}

export function formatCompact(value) {
    if (get(privacyMode)) return PRIVACY_MASK;
    const num = typeof value === 'string' ? parseFloat(value) : value;
    if (isNaN(num)) return '$0';
    if (Math.abs(num) >= 1000) {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
            notation: 'compact',
            maximumFractionDigits: 1
        }).format(num);
    }
    return formatCurrency(num);
}

export function formatDate(dateStr) {
    if (!dateStr) return '';
    const d = new Date(dateStr + 'T00:00:00');
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

export function formatDateShort(dateStr) {
    if (!dateStr) return '';
    const d = new Date(dateStr + 'T00:00:00');
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

/**
 * Format a date string as "Mon D YYYY" (e.g., "Apr 7 2027").
 * Used for recurring/subscription dates where year context matters.
 * Accepts both date-only ("2026-04-12") and full ISO ("2026-04-12T10:30:00").
 */
export function formatDateWithYear(dateStr) {
    if (!dateStr) return '';
    // If it's already a datetime string (contains 'T'), use as-is;
    // otherwise append T00:00:00 to avoid timezone-shift issues with date-only strings
    const normalized = dateStr.includes('T') ? dateStr : dateStr + 'T00:00:00';
    const d = new Date(normalized);
    if (isNaN(d.getTime())) return '';
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

export function formatDayHeader(dateStr) {
    if (!dateStr) return '';
    const d = new Date(dateStr + 'T00:00:00');
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);
    const target = new Date(d);
    target.setHours(0, 0, 0, 0);

    if (target.getTime() === today.getTime()) return 'Today';
    if (target.getTime() === yesterday.getTime()) return 'Yesterday';

    return d.toLocaleDateString('en-US', { weekday: 'long', month: 'short', day: 'numeric' });
}

export function formatMonth(monthStr) {
    if (!monthStr) return '';
    const [year, month] = monthStr.split('-');
    const d = new Date(parseInt(year), parseInt(month) - 1);
    return d.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
}

export function formatMonthShort(monthStr) {
    if (!monthStr) return '';
    const [year, month] = monthStr.split('-');
    const d = new Date(parseInt(year), parseInt(month) - 1);
    return d.toLocaleDateString('en-US', { month: 'short', year: '2-digit' });
}

export function formatPercent(value) {
    return `${value.toFixed(1)}%`;
}

export function relativeTime(isoString) {
    const now = new Date();
    const then = new Date(isoString);
    const diffMs = now - then;
    const diffMins = Math.floor(diffMs / 60000);

    if (diffMins < 1) return 'just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    const diffHrs = Math.floor(diffMins / 60);
    if (diffHrs < 24) return `${diffHrs}h ago`;
    const diffDays = Math.floor(diffHrs / 24);
    return `${diffDays}d ago`;
}

export function getCurrentMonth() {
    const now = new Date();
    return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
}

export function getPreviousMonth(monthStr) {
    const [year, month] = monthStr.split('-').map(Number);
    const d = new Date(year, month - 2);
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
}

export function groupTransactionsByDate(transactions) {
    const groups = {};
    for (const tx of transactions) {
        const date = tx.date;
        if (!groups[date]) groups[date] = [];
        groups[date].push(tx);
    }
    return Object.entries(groups).sort(([a], [b]) => b.localeCompare(a));
}

export function computeDelta(current, previous) {
    if (!previous || previous === 0) return null;
    return ((current - previous) / Math.abs(previous)) * 100;
}

export function getGreeting() {
    const hour = new Date().getHours();
    if (hour < 5) return 'Good night';
    if (hour < 12) return 'Good morning';
    if (hour < 17) return 'Good afternoon';
    if (hour < 21) return 'Good evening';
    return 'Good night';
}

/**
 * Compute trailing average savings rate over the last N months.
 * Falls back to current month if not enough history.
 */
export function computeTrailingSavingsRate(monthlyData, windowSize = 3) {
    if (!monthlyData || monthlyData.length === 0) return { rate: 0, delta: 0, months: 0 };
    const sorted = [...monthlyData].sort((a, b) => b.month.localeCompare(a.month));
    const recent = sorted.slice(0, windowSize);
    const totalIncome = recent.reduce((s, m) => s + (m.income || 0), 0);
    const totalExpenses = recent.reduce((s, m) => s + (m.expenses || 0), 0);

    if (totalIncome <= 0) return { rate: 0, delta: 0, months: recent.length };
    const savings = Math.max(totalIncome - totalExpenses, 0);
    const rate = (savings / totalIncome) * 100;

    // Compare to previous window
    const prev = sorted.slice(windowSize, windowSize * 2);
    let delta = 0;
    if (prev.length > 0) {
        const prevIncome = prev.reduce((s, m) => s + (m.income || 0), 0);
        const prevExpenses = prev.reduce((s, m) => s + (m.expenses || 0), 0);
        if (prevIncome > 0) {
            const prevSavings = Math.max(prevIncome - prevExpenses, 0);
            const prevRate = (prevSavings / prevIncome) * 100;
            delta = rate - prevRate;
        }
    }

    return {
        rate: Math.min(Math.max(rate, 0), 100),
        delta,
        months: recent.length
    };
}

/**
 * Detect recurring transactions from a list.
 * Returns upcoming charges with estimated next date.
 */
export function detectRecurring(transactions, limit = 5) {
    if (!transactions || transactions.length === 0) return [];

    // Group by description (normalized)
    const groups = {};
    for (const tx of transactions) {
        const amt = parseFloat(tx.amount);
        if (amt >= 0) continue; // Only outflows
        const key = tx.description.toLowerCase().trim().replace(/\s+/g, ' ');
        if (!groups[key]) groups[key] = [];
        groups[key].push(tx);
    }

    const recurring = [];
    const now = new Date();

    for (const [key, txns] of Object.entries(groups)) {
        if (txns.length < 2) continue;

        // Sort by date
        const sorted = txns.sort((a, b) => a.date.localeCompare(b.date));
        const dates = sorted.map(t => new Date(t.date + 'T00:00:00'));

        // Compute intervals in days
        const intervals = [];
        for (let i = 1; i < dates.length; i++) {
            const diffDays = Math.round((dates[i] - dates[i - 1]) / (1000 * 60 * 60 * 24));
            if (diffDays > 0) intervals.push(diffDays);
        }

        if (intervals.length === 0) continue;

        const avgInterval = intervals.reduce((s, d) => s + d, 0) / intervals.length;
        const stdDev = Math.sqrt(intervals.reduce((s, d) => s + (d - avgInterval) ** 2, 0) / intervals.length);

        // Consider recurring if consistent (std dev < 35% of avg, avg between 14 and 62 days)
        if (stdDev > avgInterval * 0.35) continue;
        if (avgInterval < 14 || avgInterval > 62) continue;

        const lastDate = dates[dates.length - 1];
        const lastAmount = Math.abs(parseFloat(sorted[sorted.length - 1].amount));
        const nextDate = new Date(lastDate);
        nextDate.setDate(nextDate.getDate() + Math.round(avgInterval));

        // Only include upcoming (within next 45 days)
        const daysUntil = Math.round((nextDate - now) / (1000 * 60 * 60 * 24));
        if (daysUntil < -7 || daysUntil > 45) continue;

        recurring.push({
            description: sorted[sorted.length - 1].description,
            amount: lastAmount,
            nextDate: nextDate.toISOString().split('T')[0],
            daysUntil,
            interval: Math.round(avgInterval),
            category: sorted[sorted.length - 1].category || 'Other',
            account: sorted[sorted.length - 1].account_name || '',
            occurrences: sorted.length
        });
    }

    // Sort by next date
    recurring.sort((a, b) => a.nextDate.localeCompare(b.nextDate));
    return recurring.slice(0, limit);
}

/**
 * Build a cash flow forecast for the next N days.
 * Returns an array of { date, projected } values.
 */
export function buildCashFlowForecast(currentBalance, dailyAvgSpend, dailyAvgIncome, days = 14) {
    const forecast = [];
    const now = new Date();
    let balance = currentBalance;

    for (let i = 0; i <= days; i++) {
        const date = new Date(now);
        date.setDate(date.getDate() + i);
        if (i > 0) {
            balance += dailyAvgIncome - dailyAvgSpend;
        }
        forecast.push({
            date: date.toISOString().split('T')[0],
            projected: Math.round(balance * 100) / 100,
            day: i
        });
    }
    return forecast;
}


export const CATEGORY_COLORS = {
    'Food & Dining':       '#DC2626',
    'Groceries':           '#C2410C',
    'Transportation':      '#CA8A04',
    'Entertainment':       '#7C3AED',
    'Shopping':            '#4F46E5',
    'Healthcare':          '#059669',
    'Utilities':           '#2563EB',
    'Housing':             '#0D9488',
    'Savings Transfer':    '#0891B2',
    'Credit Card Payment': '#475569',
    'Income':              '#059669',
    'Personal Transfer':   '#64748B',
    'Subscriptions':       '#C026D3',
    'Fees & Charges':      '#DC2626',
    'Travel':              '#0E7490',
    'Taxes':               '#57534E',
    'Insurance':           '#475569',
    'Other':               '#475569'
};

export const CATEGORY_ICONS = {
    'Food & Dining':       'restaurant',
    'Groceries':           'shopping_cart',
    'Transportation':      'directions_car',
    'Entertainment':       'movie',
    'Shopping':            'shopping_bag',
    'Healthcare':          'health_and_safety',
    'Utilities':           'bolt',
    'Housing':             'home',
    'Savings Transfer':    'savings',
    'Credit Card Payment': 'credit_card',
    'Income':              'payments',
    'Personal Transfer':   'swap_horiz',
    'Subscriptions':       'subscriptions',
    'Fees & Charges':      'receipt',
    'Travel':              'flight',
    'Taxes':               'account_balance',
    'Insurance':           'shield',
    'Other':               'more_horiz'
};

/**
 * Svelte action: spring-physics number counter with slight overshoot.
 * Usage: <span use:springCount={{ value: 1234, format: formatCurrency }}>$0</span>
 */
export function springCount(node, params = {}) {
    const duration = 1000;
    const overshoot = 1.06;

    function springEase(t) {
        if (t < 0.7) {
            const p = t / 0.7;
            return p * p * (3 - 2 * p) * overshoot;
        } else {
            const p = (t - 0.7) / 0.3;
            const ease = p * p * (3 - 2 * p);
            return overshoot + (1.0 - overshoot) * ease;
        }
    }

    function animate(target, formatFn) {
        const start = performance.now();
        function tick(now) {
            const elapsed = now - start;
            const t = Math.min(elapsed / duration, 1);
            const progress = springEase(t);
            const current = target * progress;
            node.textContent = formatFn ? formatFn(Math.round(current)) : Math.round(current).toLocaleString();
            if (t < 1) requestAnimationFrame(tick);
            else node.textContent = formatFn ? formatFn(target) : target.toLocaleString();
        }
        requestAnimationFrame(tick);
    }

    animate(params.value || 0, params.format || null);

    return {
        update(newParams) {
            animate(newParams.value || 0, newParams.format || null);
        }
    };
}