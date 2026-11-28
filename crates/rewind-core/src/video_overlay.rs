//! Deterministic pixel-level video overlays for `rewind cluster video`:
//!
//! - Virtual-time HUD badge (`VT mm:ss.mmm`) in the top-right corner.
//! - Optional paravirtual HID overlay (`--show-input`):
//!   - Crisp high-DPI arrow cursor with outline + drop shadow at the exact
//!     recorded `EV_ABS`/`EV_REL` position.
//!   - Expanding anti-aliased click ripples (color-coded by button: left
//!     amber-orange, right violet, middle teal) plus an active hold halo while a
//!     button is depressed.
//!   - Sleek bottom-center keycast HUD pill rendering typed characters and
//!     framed keycap badges (`Tab`, `Enter`, `Esc`, `Bksp`, `Ctrl+C`, ...)
//!     with smooth fade-out.
//!
//! All rendering is pure safe Rust directly into the `bgr0` framebuffer slice
//! using the recorded `FaultKind::HidEvents` timestamps — zero external font or
//! graphics dependencies and 100% deterministic across platforms.

use rewind_vmm::net::{FaultKind, HidEvent, ScheduledFault};

const EV_KEY: u16 = 0x01;
const EV_REL: u16 = 0x02;
const EV_ABS: u16 = 0x03;

const ABS_X: u16 = 0x00;
const ABS_Y: u16 = 0x01;
const REL_X: u16 = 0x00;
const REL_Y: u16 = 0x01;

const BTN_LEFT: u16 = 0x110;
const BTN_RIGHT: u16 = 0x111;
const BTN_MIDDLE: u16 = 0x112;

const KEY_LEFTSHIFT: u16 = 42;
const KEY_RIGHTSHIFT: u16 = 54;
const KEY_LEFTCTRL: u16 = 29;
const KEY_RIGHTCTRL: u16 = 97;
const KEY_LEFTALT: u16 = 56;
const KEY_RIGHTALT: u16 = 100;
const KEY_LEFTMETA: u16 = 125;
const KEY_RIGHTMETA: u16 = 126;

const RIPPLE_DURATION_NS: u64 = 450_000_000;
const KEY_DISPLAY_NS: u64 = 2_200_000_000;
const KEY_FADE_NS: u64 = 450_000_000;
const MAX_KEY_TOKENS: usize = 28;

/// Classic 14×19 desktop pointer arrow (`B` = dark outline, `W` = white fill).
/// Hotspot is `(0, 0)` (the sharp tip).
const CURSOR_SPRITE: &[&[u8; 14]] = &[
    b"B.............",
    b"BB............",
    b"BWB...........",
    b"BWWB..........",
    b"BWWWB.........",
    b"BWWWWB........",
    b"BWWWWWB.......",
    b"BWWWWWWB......",
    b"BWWWWWWWB.....",
    b"BWWWWWWWWB....",
    b"BWWWWWWWWWB...",
    b"BWWWWWWWWWWB..",
    b"BWWWWWWBBBBB..",
    b"BWWWBWWB......",
    b"BWWB.BWWB.....",
    b"BWB..BWWB.....",
    b"BB....BWWB....",
    b"B.....BWWB....",
    b".......BB.....",
];

#[derive(Clone, Copy, Debug)]
struct TimedHidEvent {
    at_ns: u64,
    event: HidEvent,
}

#[derive(Clone, Copy, Debug)]
struct ClickRipple {
    x: i32,
    y: i32,
    at_ns: u64,
    button: u16,
}

#[derive(Clone, Debug)]
enum KeyToken {
    Char(char),
    Badge(String),
}

#[derive(Clone, Debug)]
struct KeyEntry {
    token: KeyToken,
    at_ns: u64,
}

/// Stateful, monotonically advancing HID overlay renderer for one node.
#[derive(Clone, Debug)]
pub(crate) struct HidOverlay {
    events: Vec<TimedHidEvent>,
    next_idx: usize,
    cursor_visible: bool,
    cursor_x: i32,
    cursor_y: i32,
    held_buttons: u8,
    shift_l: bool,
    shift_r: bool,
    ctrl_l: bool,
    ctrl_r: bool,
    alt_l: bool,
    alt_r: bool,
    meta_l: bool,
    meta_r: bool,
    ripples: Vec<ClickRipple>,
    keys: Vec<KeyEntry>,
    clear_keys_on_next_key: bool,
}

impl HidOverlay {
    /// Extracts all recorded `FaultKind::HidEvents` targeting `node_idx`.
    pub(crate) fn from_faults(faults: &[ScheduledFault], node_idx: usize) -> HidOverlay {
        let mut events = Vec::new();
        for f in faults {
            if let FaultKind::HidEvents { node, events: evs } = &f.kind
                && *node == node_idx
            {
                for &event in evs {
                    events.push(TimedHidEvent {
                        at_ns: f.at_ns,
                        event,
                    });
                }
            }
        }
        // Stable sort preserves exact intra-timestamp event order.
        events.sort_by_key(|e| e.at_ns);
        HidOverlay {
            events,
            next_idx: 0,
            cursor_visible: false,
            cursor_x: 0,
            cursor_y: 0,
            held_buttons: 0,
            shift_l: false,
            shift_r: false,
            ctrl_l: false,
            ctrl_r: false,
            alt_l: false,
            alt_r: false,
            meta_l: false,
            meta_r: false,
            ripples: Vec::new(),
            keys: Vec::new(),
            clear_keys_on_next_key: false,
        }
    }

    fn shift(&self) -> bool {
        self.shift_l || self.shift_r
    }

    fn ctrl(&self) -> bool {
        self.ctrl_l || self.ctrl_r
    }

    fn alt(&self) -> bool {
        self.alt_l || self.alt_r
    }

    fn meta(&self) -> bool {
        self.meta_l || self.meta_r
    }

    /// Consumes all HID events with `at_ns <= now_ns` and updates state.
    pub(crate) fn advance_to(&mut self, now_ns: u64, width: u32, height: u32) {
        let max_x = width.saturating_sub(1) as i32;
        let max_y = height.saturating_sub(1) as i32;

        while self.next_idx < self.events.len() && self.events[self.next_idx].at_ns <= now_ns {
            let TimedHidEvent { at_ns, event } = self.events[self.next_idx];
            self.next_idx += 1;
            match event.kind {
                EV_ABS => match event.code {
                    ABS_X => {
                        let v = event.value.clamp(0, 32767) as u64;
                        self.cursor_x = ((v * max_x as u64 + 16383) / 32767) as i32;
                        self.cursor_visible = true;
                    }
                    ABS_Y => {
                        let v = event.value.clamp(0, 32767) as u64;
                        self.cursor_y = ((v * max_y as u64 + 16383) / 32767) as i32;
                        self.cursor_visible = true;
                    }
                    _ => {}
                },
                EV_REL => match event.code {
                    REL_X => {
                        self.cursor_x = (self.cursor_x + event.value).clamp(0, max_x);
                        self.cursor_visible = true;
                    }
                    REL_Y => {
                        self.cursor_y = (self.cursor_y + event.value).clamp(0, max_y);
                        self.cursor_visible = true;
                    }
                    _ => {}
                },
                EV_KEY => {
                    let pressed = event.value != 0;
                    match event.code {
                        BTN_LEFT | BTN_RIGHT | BTN_MIDDLE => {
                            let bit = match event.code {
                                BTN_LEFT => 1u8,
                                BTN_RIGHT => 2u8,
                                _ => 4u8,
                            };
                            if pressed {
                                self.held_buttons |= bit;
                                self.cursor_visible = true;
                                self.clear_keys_on_next_key = true;
                                self.ripples.push(ClickRipple {
                                    x: self.cursor_x,
                                    y: self.cursor_y,
                                    at_ns,
                                    button: event.code,
                                });
                            } else {
                                self.held_buttons &= !bit;
                            }
                        }
                        KEY_LEFTSHIFT => self.shift_l = pressed,
                        KEY_RIGHTSHIFT => self.shift_r = pressed,
                        KEY_LEFTCTRL => self.ctrl_l = pressed,
                        KEY_RIGHTCTRL => self.ctrl_r = pressed,
                        KEY_LEFTALT => self.alt_l = pressed,
                        KEY_RIGHTALT => self.alt_r = pressed,
                        KEY_LEFTMETA => self.meta_l = pressed,
                        KEY_RIGHTMETA => self.meta_r = pressed,
                        code if event.value == 1 => {
                            if let Some(token) = decode_key_token(
                                code,
                                self.shift(),
                                self.ctrl(),
                                self.alt(),
                                self.meta(),
                            ) {
                                if self.clear_keys_on_next_key {
                                    self.keys.clear();
                                    self.clear_keys_on_next_key = false;
                                }
                                self.keys.push(KeyEntry { token, at_ns });
                            }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        // Prune expired ripples and key entries.
        self.ripples
            .retain(|r| now_ns >= r.at_ns && now_ns - r.at_ns <= RIPPLE_DURATION_NS);
        self.keys
            .retain(|k| now_ns >= k.at_ns && now_ns - k.at_ns <= KEY_DISPLAY_NS);
        if self.keys.len() > MAX_KEY_TOKENS {
            let drain = self.keys.len() - MAX_KEY_TOKENS;
            self.keys.drain(..drain);
        }
    }

    /// Draws click ripples, cursor, and keycast HUD onto a packed `bgr0` frame.
    pub(crate) fn draw(&mut self, buf: &mut [u8], width: u32, height: u32, now_ns: u64) {
        self.advance_to(now_ns, width, height);

        // 1. Click ripples (under the pointer tip).
        for ripple in &self.ripples {
            let age = now_ns.saturating_sub(ripple.at_ns);
            if age > RIPPLE_DURATION_NS {
                continue;
            }
            let t = age as f32 / RIPPLE_DURATION_NS as f32;
            let eased = 1.0 - (1.0 - t) * (1.0 - t);
            let radius = 5.0 + 26.0 * eased;
            let ring_alpha = (1.0 - t) * 0.92;
            let fill_alpha = (1.0 - t) * (1.0 - t) * 0.28;
            let rgb = match ripple.button {
                BTN_LEFT => (255u8, 122u8, 24u8),  // vibrant amber-orange
                BTN_RIGHT => (129u8, 140u8, 248u8), // crisp indigo-violet
                _ => (45u8, 212u8, 191u8),          // bright teal
            };
            draw_click_ripple(
                buf,
                width,
                height,
                ripple.x,
                ripple.y,
                radius,
                rgb,
                ring_alpha,
                fill_alpha,
            );
        }

        // Active button-held halo while depressed.
        if self.cursor_visible && self.held_buttons != 0 {
            let rgb = if (self.held_buttons & 1) != 0 {
                (255u8, 122u8, 24u8)
            } else if (self.held_buttons & 2) != 0 {
                (129u8, 140u8, 248u8)
            } else {
                (45u8, 212u8, 191u8)
            };
            draw_click_ripple(
                buf,
                width,
                height,
                self.cursor_x,
                self.cursor_y,
                7.5,
                rgb,
                0.70,
                0.18,
            );
        }

        // 2. Crisp arrow pointer at `(cursor_x, cursor_y)`.
        if self.cursor_visible {
            draw_cursor(buf, width, height, self.cursor_x, self.cursor_y);
        }

        // 3. Keycast HUD pill at bottom-center.
        if !self.keys.is_empty() {
            draw_keycast_hud(buf, width, height, &self.keys, now_ns);
        }
    }
}

#[inline]
fn blend_pixel(buf: &mut [u8], width: u32, height: u32, x: i32, y: i32, rgb: (u8, u8, u8), alpha: f32) {
    if x < 0 || y < 0 || x >= width as i32 || y >= height as i32 || alpha <= 0.001 {
        return;
    }
    let a = alpha.clamp(0.0, 1.0);
    let inv = 1.0 - a;
    let off = ((y as usize) * (width as usize) + (x as usize)) * 4;
    if off + 4 <= buf.len() {
        let (r, g, b) = rgb;
        buf[off] = (buf[off] as f32 * inv + b as f32 * a).round() as u8;
        buf[off + 1] = (buf[off + 1] as f32 * inv + g as f32 * a).round() as u8;
        buf[off + 2] = (buf[off + 2] as f32 * inv + r as f32 * a).round() as u8;
        buf[off + 3] = 0;
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "private pixel rasterization helper taking framebuffer slice, bounds, geometry, and color"
)]
fn draw_click_ripple(
    buf: &mut [u8],
    width: u32,
    height: u32,
    cx: i32,
    cy: i32,
    radius: f32,
    rgb: (u8, u8, u8),
    ring_alpha: f32,
    fill_alpha: f32,
) {
    let thickness = 2.4f32;
    let bound = (radius + thickness + 2.0).ceil() as i32;
    for dy in -bound..=bound {
        let py = cy + dy;
        if py < 0 || py >= height as i32 {
            continue;
        }
        for dx in -bound..=bound {
            let px = cx + dx;
            if px < 0 || px >= width as i32 {
                continue;
            }
            let d = ((dx * dx + dy * dy) as f32).sqrt();
            let ring_dist = (d - radius).abs();
            let mut a = 0.0f32;
            if d < radius {
                a = a.max(fill_alpha);
            }
            if ring_dist < thickness {
                let cov = (1.0 - ring_dist / thickness).clamp(0.0, 1.0);
                a = a.max(ring_alpha * cov);
            }
            if a > 0.002 {
                blend_pixel(buf, width, height, px, py, rgb, a);
            }
        }
    }
}

fn draw_cursor(buf: &mut [u8], width: u32, height: u32, cx: i32, cy: i32) {
    // Soft drop shadow first (`+1, +2`).
    for (row_idx, row) in CURSOR_SPRITE.iter().enumerate() {
        for (col_idx, &px) in row.iter().enumerate() {
            if px != b'.' {
                blend_pixel(
                    buf,
                    width,
                    height,
                    cx + col_idx as i32 + 1,
                    cy + row_idx as i32 + 2,
                    (0, 0, 0),
                    0.38,
                );
            }
        }
    }
    // Crisp dark border + pure white fill.
    for (row_idx, row) in CURSOR_SPRITE.iter().enumerate() {
        for (col_idx, &px) in row.iter().enumerate() {
            match px {
                b'B' => blend_pixel(
                    buf,
                    width,
                    height,
                    cx + col_idx as i32,
                    cy + row_idx as i32,
                    (15, 23, 42),
                    1.0,
                ),
                b'W' => blend_pixel(
                    buf,
                    width,
                    height,
                    cx + col_idx as i32,
                    cy + row_idx as i32,
                    (255, 255, 255),
                    1.0,
                ),
                _ => {}
            }
        }
    }
}

enum HudSegment {
    Text(String),
    Badge(String),
}

fn draw_keycast_hud(
    buf: &mut [u8],
    width: u32,
    height: u32,
    keys: &[KeyEntry],
    now_ns: u64,
) {
    let newest_ns = keys.last().map_or(now_ns, |k| k.at_ns);
    let newest_age = now_ns.saturating_sub(newest_ns);
    if newest_age >= KEY_DISPLAY_NS {
        return;
    }
    let fade = if newest_age + KEY_FADE_NS > KEY_DISPLAY_NS {
        (KEY_DISPLAY_NS - newest_age) as f32 / KEY_FADE_NS as f32
    } else {
        1.0
    };

    // Merge consecutive printable characters into text runs while keeping
    // special keys / chords as framed keycap badges.
    let mut segments: Vec<HudSegment> = Vec::new();
    for entry in keys {
        match &entry.token {
            KeyToken::Char(ch) => {
                if let Some(HudSegment::Text(s)) = segments.last_mut() {
                    s.push(*ch);
                } else {
                    segments.push(HudSegment::Text(ch.to_string()));
                }
            }
            KeyToken::Badge(label) => {
                segments.push(HudSegment::Badge(label.clone()));
            }
        }
    }
    if segments.is_empty() {
        return;
    }

    let scale = 2i32;
    let char_w = 6 * scale;
    let char_h = 7 * scale;
    let seg_gap = 10i32;
    let badge_pad_x = 8i32;
    let badge_pad_y = 4i32;
    let outer_pad_x = 16i32;
    let outer_pad_y = 10i32;

    let mut content_w = 0i32;
    for (i, seg) in segments.iter().enumerate() {
        if i > 0 {
            content_w += seg_gap;
        }
        content_w += match seg {
            HudSegment::Text(s) => (s.len() as i32) * char_w - scale,
            HudSegment::Badge(s) => (s.len() as i32) * char_w - scale + badge_pad_x * 2,
        };
    }

    let box_w = content_w + outer_pad_x * 2;
    let box_h = char_h + badge_pad_y * 2 + outer_pad_y * 2;
    let w = width as i32;
    let h = height as i32;
    if box_w + 24 > w || box_h + 24 > h {
        return;
    }

    let x0 = (w - box_w) / 2;
    let y0 = h - box_h - 24;

    // Outer translucent glass container + crisp 1px slate border.
    fill_rect_alpha(buf, width, height, x0, y0, box_w, box_h, (11, 18, 32), 0.84 * fade);
    stroke_rect_alpha(buf, width, height, x0, y0, box_w, box_h, (71, 85, 105), 0.80 * fade);

    let mut cur_x = x0 + outer_pad_x;
    let text_y = y0 + outer_pad_y + badge_pad_y;

    for (i, seg) in segments.iter().enumerate() {
        if i > 0 {
            cur_x += seg_gap;
        }
        match seg {
            HudSegment::Text(s) => {
                draw_text_alpha(
                    buf,
                    width,
                    height,
                    cur_x,
                    text_y,
                    s,
                    scale,
                    (248, 250, 252),
                    fade,
                );
                cur_x += (s.len() as i32) * char_w - scale;
            }
            HudSegment::Badge(label) => {
                let bw = (label.len() as i32) * char_w - scale + badge_pad_x * 2;
                let bh = char_h + badge_pad_y * 2;
                let by = y0 + outer_pad_y;
                fill_rect_alpha(buf, width, height, cur_x, by, bw, bh, (30, 41, 59), 0.94 * fade);
                stroke_rect_alpha(buf, width, height, cur_x, by, bw, bh, (100, 116, 139), 0.90 * fade);
                draw_text_alpha(
                    buf,
                    width,
                    height,
                    cur_x + badge_pad_x,
                    text_y,
                    label,
                    scale,
                    (56, 189, 248), // bright sky-cyan for special keys
                    fade,
                );
                cur_x += bw;
            }
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "private pixel rasterization helper taking framebuffer slice, bounds, geometry, and color"
)]
fn fill_rect_alpha(
    buf: &mut [u8],
    width: u32,
    height: u32,
    x0: i32,
    y0: i32,
    w: i32,
    h: i32,
    rgb: (u8, u8, u8),
    alpha: f32,
) {
    for y in y0..(y0 + h) {
        for x in x0..(x0 + w) {
            blend_pixel(buf, width, height, x, y, rgb, alpha);
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "private pixel rasterization helper taking framebuffer slice, bounds, geometry, and color"
)]
fn stroke_rect_alpha(
    buf: &mut [u8],
    width: u32,
    height: u32,
    x0: i32,
    y0: i32,
    w: i32,
    h: i32,
    rgb: (u8, u8, u8),
    alpha: f32,
) {
    if w <= 0 || h <= 0 {
        return;
    }
    for x in x0..(x0 + w) {
        blend_pixel(buf, width, height, x, y0, rgb, alpha);
        blend_pixel(buf, width, height, x, y0 + h - 1, rgb, alpha);
    }
    for y in (y0 + 1)..(y0 + h - 1) {
        blend_pixel(buf, width, height, x0, y, rgb, alpha);
        blend_pixel(buf, width, height, x0 + w - 1, y, rgb, alpha);
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "private pixel rasterization helper taking framebuffer slice, bounds, geometry, and color"
)]
fn draw_text_alpha(
    buf: &mut [u8],
    width: u32,
    height: u32,
    x0: i32,
    y0: i32,
    text: &str,
    scale: i32,
    rgb: (u8, u8, u8),
    alpha: f32,
) {
    let char_w = 6 * scale;
    for (ci, ch) in text.bytes().enumerate() {
        let glyph = ascii_glyph_5x7(ch);
        let gx = x0 + (ci as i32) * char_w;
        for (row, bits) in glyph.iter().enumerate() {
            for col in 0..5i32 {
                if (bits & (1 << (4 - col))) != 0 {
                    for sy in 0..scale {
                        for sx in 0..scale {
                            blend_pixel(
                                buf,
                                width,
                                height,
                                gx + col * scale + sx,
                                y0 + (row as i32) * scale + sy,
                                rgb,
                                alpha,
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Stamps a crisp translucent virtual-time badge (`VT 00:04.233`) in the
/// top-right corner of a packed `bgr0` frame.
pub(crate) fn draw_timestamp_hud(buf: &mut [u8], width: u32, height: u32, ns: u64) {
    let total_ms = ns / 1_000_000;
    let mins = (total_ms / 60_000) % 100;
    let secs = (total_ms / 1_000) % 60;
    let ms = total_ms % 1_000;
    let label = format!("VT {mins:02}:{secs:02}.{ms:03}");

    let scale = 2i32;
    let char_w = 6 * scale;
    let char_h = 7 * scale;
    let pad_x = 10i32;
    let pad_y = 6i32;
    let box_w = (label.len() as i32) * char_w + pad_x * 2 - scale;
    let box_h = char_h + pad_y * 2;
    let margin = 12i32;
    let w = width as i32;
    let h = height as i32;
    if w < box_w + margin * 2 || h < box_h + margin * 2 {
        return;
    }
    let x0 = w - box_w - margin;
    let y0 = margin;

    fill_rect_alpha(buf, width, height, x0, y0, box_w, box_h, (0, 0, 0), 0.75);
    draw_text_alpha(
        buf,
        width,
        height,
        x0 + pad_x,
        y0 + pad_y,
        &label,
        scale,
        (240, 248, 255),
        1.0,
    );
}

fn decode_key_token(
    code: u16,
    shift: bool,
    ctrl: bool,
    alt: bool,
    meta: bool,
) -> Option<KeyToken> {
    let printable: Option<(char, char)> = match code {
        2 => Some(('1', '!')),
        3 => Some(('2', '@')),
        4 => Some(('3', '#')),
        5 => Some(('4', '$')),
        6 => Some(('5', '%')),
        7 => Some(('6', '^')),
        8 => Some(('7', '&')),
        9 => Some(('8', '*')),
        10 => Some(('9', '(')),
        11 => Some(('0', ')')),
        12 => Some(('-', '_')),
        13 => Some(('=', '+')),
        16 => Some(('q', 'Q')),
        17 => Some(('w', 'W')),
        18 => Some(('e', 'E')),
        19 => Some(('r', 'R')),
        20 => Some(('t', 'T')),
        21 => Some(('y', 'Y')),
        22 => Some(('u', 'U')),
        23 => Some(('i', 'I')),
        24 => Some(('o', 'O')),
        25 => Some(('p', 'P')),
        26 => Some(('[', '{')),
        27 => Some((']', '}')),
        30 => Some(('a', 'A')),
        31 => Some(('s', 'S')),
        32 => Some(('d', 'D')),
        33 => Some(('f', 'F')),
        34 => Some(('g', 'G')),
        35 => Some(('h', 'H')),
        36 => Some(('j', 'J')),
        37 => Some(('k', 'K')),
        38 => Some(('l', 'L')),
        39 => Some((';', ':')),
        40 => Some(('\'', '"')),
        41 => Some(('`', '~')),
        43 => Some(('\\', '|')),
        44 => Some(('z', 'Z')),
        45 => Some(('x', 'X')),
        46 => Some(('c', 'C')),
        47 => Some(('v', 'V')),
        48 => Some(('b', 'B')),
        49 => Some(('n', 'N')),
        50 => Some(('m', 'M')),
        51 => Some((',', '<')),
        52 => Some(('.', '>')),
        53 => Some(('/', '?')),
        57 => Some((' ', ' ')),
        _ => None,
    };

    let special: Option<&'static str> = match code {
        1 => Some("Esc"),
        14 => Some("Bksp"),
        15 => Some("Tab"),
        28 | 96 => Some("Enter"),
        59 => Some("F1"),
        60 => Some("F2"),
        61 => Some("F3"),
        62 => Some("F4"),
        63 => Some("F5"),
        64 => Some("F6"),
        65 => Some("F7"),
        66 => Some("F8"),
        67 => Some("F9"),
        68 => Some("F10"),
        87 => Some("F11"),
        88 => Some("F12"),
        102 => Some("Home"),
        103 => Some("Up"),
        104 => Some("PgUp"),
        105 => Some("Left"),
        106 => Some("Right"),
        107 => Some("End"),
        108 => Some("Down"),
        109 => Some("PgDn"),
        111 => Some("Del"),
        _ => None,
    };

    let has_chord_mod = ctrl || alt || meta;
    if has_chord_mod {
        let mut parts = Vec::new();
        if ctrl {
            parts.push("Ctrl".to_string());
        }
        if alt {
            parts.push("Alt".to_string());
        }
        if meta {
            parts.push("Super".to_string());
        }
        if shift {
            parts.push("Shift".to_string());
        }
        let key_label = if let Some(sp) = special {
            sp.to_string()
        } else if let Some((lo, hi)) = printable {
            if lo == ' ' {
                "Space".to_string()
            } else {
                hi.to_string()
            }
        } else {
            return None;
        };
        parts.push(key_label);
        return Some(KeyToken::Badge(parts.join("+")));
    }

    if let Some(sp) = special {
        if shift {
            return Some(KeyToken::Badge(format!("Shift+{sp}")));
        }
        return Some(KeyToken::Badge(sp.to_string()));
    }
    if let Some((lo, hi)) = printable {
        return Some(KeyToken::Char(if shift { hi } else { lo }));
    }
    None
}

/// Complete printable ASCII (`0x20..=0x7E`) crisp 5×7 pixel bitmap font.
fn ascii_glyph_5x7(ch: u8) -> [u8; 7] {
    match ch {
        b' ' => [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        b'!' => [0x04, 0x04, 0x04, 0x04, 0x04, 0x00, 0x04],
        b'"' => [0x0A, 0x0A, 0x0A, 0x00, 0x00, 0x00, 0x00],
        b'#' => [0x0A, 0x0A, 0x1F, 0x0A, 0x1F, 0x0A, 0x0A],
        b'$' => [0x04, 0x0F, 0x14, 0x0E, 0x05, 0x1E, 0x04],
        b'%' => [0x18, 0x19, 0x02, 0x04, 0x08, 0x13, 0x03],
        b'&' => [0x0C, 0x12, 0x14, 0x08, 0x15, 0x12, 0x0D],
        b'\'' => [0x04, 0x04, 0x08, 0x00, 0x00, 0x00, 0x00],
        b'(' => [0x02, 0x04, 0x08, 0x08, 0x08, 0x04, 0x02],
        b')' => [0x08, 0x04, 0x02, 0x02, 0x02, 0x04, 0x08],
        b'*' => [0x00, 0x04, 0x15, 0x0E, 0x15, 0x04, 0x00],
        b'+' => [0x00, 0x04, 0x04, 0x1F, 0x04, 0x04, 0x00],
        b',' => [0x00, 0x00, 0x00, 0x00, 0x04, 0x04, 0x08],
        b'-' => [0x00, 0x00, 0x00, 0x1F, 0x00, 0x00, 0x00],
        b'.' => [0x00, 0x00, 0x00, 0x00, 0x00, 0x0C, 0x0C],
        b'/' => [0x01, 0x01, 0x02, 0x04, 0x08, 0x10, 0x10],
        b'0' => [0x0E, 0x11, 0x13, 0x15, 0x19, 0x11, 0x0E],
        b'1' => [0x04, 0x0C, 0x04, 0x04, 0x04, 0x04, 0x0E],
        b'2' => [0x0E, 0x11, 0x01, 0x02, 0x04, 0x08, 0x1F],
        b'3' => [0x1F, 0x02, 0x04, 0x02, 0x01, 0x11, 0x0E],
        b'4' => [0x02, 0x06, 0x0A, 0x12, 0x1F, 0x02, 0x02],
        b'5' => [0x1F, 0x10, 0x1E, 0x01, 0x01, 0x11, 0x0E],
        b'6' => [0x06, 0x08, 0x10, 0x1E, 0x11, 0x11, 0x0E],
        b'7' => [0x1F, 0x01, 0x02, 0x04, 0x08, 0x08, 0x08],
        b'8' => [0x0E, 0x11, 0x11, 0x0E, 0x11, 0x11, 0x0E],
        b'9' => [0x0E, 0x11, 0x11, 0x0F, 0x01, 0x02, 0x0C],
        b':' => [0x00, 0x04, 0x04, 0x00, 0x04, 0x04, 0x00],
        b';' => [0x00, 0x04, 0x04, 0x00, 0x04, 0x04, 0x08],
        b'<' => [0x02, 0x04, 0x08, 0x10, 0x08, 0x04, 0x02],
        b'=' => [0x00, 0x00, 0x1F, 0x00, 0x1F, 0x00, 0x00],
        b'>' => [0x08, 0x04, 0x02, 0x01, 0x02, 0x04, 0x08],
        b'?' => [0x0E, 0x11, 0x01, 0x02, 0x04, 0x00, 0x04],
        b'@' => [0x0E, 0x11, 0x17, 0x15, 0x17, 0x10, 0x0E],
        b'A' => [0x0E, 0x11, 0x11, 0x1F, 0x11, 0x11, 0x11],
        b'B' => [0x1E, 0x11, 0x11, 0x1E, 0x11, 0x11, 0x1E],
        b'C' => [0x0E, 0x11, 0x10, 0x10, 0x10, 0x11, 0x0E],
        b'D' => [0x1C, 0x12, 0x11, 0x11, 0x11, 0x12, 0x1C],
        b'E' => [0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x1F],
        b'F' => [0x1F, 0x10, 0x10, 0x1E, 0x10, 0x10, 0x10],
        b'G' => [0x0E, 0x11, 0x10, 0x17, 0x11, 0x11, 0x0F],
        b'H' => [0x11, 0x11, 0x11, 0x1F, 0x11, 0x11, 0x11],
        b'I' => [0x0E, 0x04, 0x04, 0x04, 0x04, 0x04, 0x0E],
        b'J' => [0x07, 0x02, 0x02, 0x02, 0x02, 0x12, 0x0C],
        b'K' => [0x11, 0x12, 0x14, 0x18, 0x14, 0x12, 0x11],
        b'L' => [0x10, 0x10, 0x10, 0x10, 0x10, 0x10, 0x1F],
        b'M' => [0x11, 0x1B, 0x15, 0x15, 0x11, 0x11, 0x11],
        b'N' => [0x11, 0x19, 0x15, 0x13, 0x11, 0x11, 0x11],
        b'O' => [0x0E, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E],
        b'P' => [0x1E, 0x11, 0x11, 0x1E, 0x10, 0x10, 0x10],
        b'Q' => [0x0E, 0x11, 0x11, 0x11, 0x15, 0x12, 0x0D],
        b'R' => [0x1E, 0x11, 0x11, 0x1E, 0x14, 0x12, 0x11],
        b'S' => [0x0F, 0x10, 0x10, 0x0E, 0x01, 0x01, 0x1E],
        b'T' => [0x1F, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04],
        b'U' => [0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x0E],
        b'V' => [0x11, 0x11, 0x11, 0x11, 0x11, 0x0A, 0x04],
        b'W' => [0x11, 0x11, 0x11, 0x15, 0x15, 0x1B, 0x11],
        b'X' => [0x11, 0x11, 0x0A, 0x04, 0x0A, 0x11, 0x11],
        b'Y' => [0x11, 0x11, 0x0A, 0x04, 0x04, 0x04, 0x04],
        b'Z' => [0x1F, 0x01, 0x02, 0x04, 0x08, 0x10, 0x1F],
        b'[' => [0x0E, 0x08, 0x08, 0x08, 0x08, 0x08, 0x0E],
        b'\\' => [0x10, 0x10, 0x08, 0x04, 0x02, 0x01, 0x01],
        b']' => [0x0E, 0x02, 0x02, 0x02, 0x02, 0x02, 0x0E],
        b'^' => [0x04, 0x0A, 0x11, 0x00, 0x00, 0x00, 0x00],
        b'_' => [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1F],
        b'`' => [0x08, 0x04, 0x02, 0x00, 0x00, 0x00, 0x00],
        b'a' => [0x00, 0x00, 0x0E, 0x01, 0x0F, 0x11, 0x0F],
        b'b' => [0x10, 0x10, 0x16, 0x19, 0x11, 0x11, 0x1E],
        b'c' => [0x00, 0x00, 0x0E, 0x10, 0x10, 0x11, 0x0E],
        b'd' => [0x01, 0x01, 0x0D, 0x13, 0x11, 0x11, 0x0F],
        b'e' => [0x00, 0x00, 0x0E, 0x11, 0x1F, 0x10, 0x0E],
        b'f' => [0x06, 0x09, 0x08, 0x1C, 0x08, 0x08, 0x08],
        b'g' => [0x00, 0x0F, 0x11, 0x11, 0x0F, 0x01, 0x0E],
        b'h' => [0x10, 0x10, 0x16, 0x19, 0x11, 0x11, 0x11],
        b'i' => [0x04, 0x00, 0x0C, 0x04, 0x04, 0x04, 0x0E],
        b'j' => [0x02, 0x00, 0x06, 0x02, 0x02, 0x12, 0x0C],
        b'k' => [0x10, 0x10, 0x12, 0x14, 0x18, 0x14, 0x12],
        b'l' => [0x0C, 0x04, 0x04, 0x04, 0x04, 0x04, 0x0E],
        b'm' => [0x00, 0x00, 0x1A, 0x15, 0x15, 0x11, 0x11],
        b'n' => [0x00, 0x00, 0x16, 0x19, 0x11, 0x11, 0x11],
        b'o' => [0x00, 0x00, 0x0E, 0x11, 0x11, 0x11, 0x0E],
        b'p' => [0x00, 0x00, 0x1E, 0x11, 0x1E, 0x10, 0x10],
        b'q' => [0x00, 0x00, 0x0D, 0x13, 0x0F, 0x01, 0x01],
        b'r' => [0x00, 0x00, 0x16, 0x19, 0x10, 0x10, 0x10],
        b's' => [0x00, 0x00, 0x0F, 0x10, 0x0E, 0x01, 0x1E],
        b't' => [0x08, 0x08, 0x1C, 0x08, 0x08, 0x09, 0x06],
        b'u' => [0x00, 0x00, 0x11, 0x11, 0x11, 0x13, 0x0D],
        b'v' => [0x00, 0x00, 0x11, 0x11, 0x11, 0x0A, 0x04],
        b'w' => [0x00, 0x00, 0x11, 0x11, 0x15, 0x15, 0x0A],
        b'x' => [0x00, 0x00, 0x11, 0x0A, 0x04, 0x0A, 0x11],
        b'y' => [0x00, 0x00, 0x11, 0x11, 0x0F, 0x01, 0x0E],
        b'z' => [0x00, 0x00, 0x1F, 0x02, 0x04, 0x08, 0x1F],
        b'{' => [0x02, 0x04, 0x04, 0x08, 0x04, 0x04, 0x02],
        b'|' => [0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04],
        b'}' => [0x08, 0x04, 0x04, 0x02, 0x04, 0x04, 0x08],
        b'~' => [0x00, 0x00, 0x08, 0x15, 0x02, 0x00, 0x00],
        _ => [0; 7],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hid_overlay_renders_cursor_click_ripple_and_keycast_deterministically() {
        let faults = vec![
            ScheduledFault {
                at_ns: 100_000_000,
                at_step: None,
                kind: FaultKind::HidEvents {
                    node: 0,
                    events: vec![
                        HidEvent {
                            kind: EV_ABS,
                            code: ABS_X,
                            value: 16384,
                        },
                        HidEvent {
                            kind: EV_ABS,
                            code: ABS_Y,
                            value: 16384,
                        },
                        HidEvent {
                            kind: EV_KEY,
                            code: BTN_LEFT,
                            value: 1,
                        },
                    ],
                },
            },
            ScheduledFault {
                at_ns: 140_000_000,
                at_step: None,
                kind: FaultKind::HidEvents {
                    node: 0,
                    events: vec![
                        HidEvent {
                            kind: EV_KEY,
                            code: BTN_LEFT,
                            value: 0,
                        },
                        HidEvent {
                            kind: EV_KEY,
                            code: KEY_LEFTSHIFT,
                            value: 1,
                        },
                        HidEvent {
                            kind: EV_KEY,
                            code: 35, // 'h' -> 'H'
                            value: 1,
                        },
                        HidEvent {
                            kind: EV_KEY,
                            code: KEY_LEFTSHIFT,
                            value: 0,
                        },
                        HidEvent {
                            kind: EV_KEY,
                            code: 23, // 'i'
                            value: 1,
                        },
                        HidEvent {
                            kind: EV_KEY,
                            code: 28, // Enter
                            value: 1,
                        },
                    ],
                },
            },
        ];

        let mut overlay_a = HidOverlay::from_faults(&faults, 0);
        let mut overlay_b = HidOverlay::from_faults(&faults, 0);
        let (w, h) = (640u32, 360u32);
        let mut frame_a = vec![0x80u8; (w * h * 4) as usize];
        let mut frame_b = vec![0x80u8; (w * h * 4) as usize];

        overlay_a.draw(&mut frame_a, w, h, 200_000_000);
        overlay_b.draw(&mut frame_b, w, h, 200_000_000);

        assert_eq!(frame_a, frame_b, "overlay rendering must be bit-for-bit deterministic");
        // Verify cursor was drawn near center `(320, 180)` (dark outline at tip `(320, 180)`).
        let tip = ((180 * w + 320) * 4) as usize;
        assert_eq!(&frame_a[tip..tip + 3], &[42, 23, 15]);
        // Verify click ripple painted non-gray pixels around `(320, 180)`.
        let mut modified_center = 0usize;
        for dy in -30..=30i32 {
            for dx in -30..=30i32 {
                let off = (((180 + dy) as u32 * w + (320 + dx) as u32) * 4) as usize;
                if frame_a[off] != 0x80 || frame_a[off + 1] != 0x80 || frame_a[off + 2] != 0x80 {
                    modified_center += 1;
                }
            }
        }
        assert!(modified_center > 100, "expected cursor + ripple pixels near center");
        // Verify keycast HUD painted pixels near bottom center (`y = h - 40`).
        let mut modified_hud = 0usize;
        for y in (h - 70)..(h - 20) {
            for x in 200..440 {
                let off = ((y * w + x) * 4) as usize;
                if frame_a[off] != 0x80 || frame_a[off + 1] != 0x80 || frame_a[off + 2] != 0x80 {
                    modified_hud += 1;
                }
            }
        }
        assert!(modified_hud > 500, "expected keycast HUD pill near bottom-center");
    }
}
