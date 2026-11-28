//! The Cluster bar and Cluster right-hand tab: multi-node switcher, synced
//! virtual-time swimlane canvas (running / crashed / paused / clogged /
//! partitioned intervals + interactive playhead), and virtual-scrolled L2
//! network + fault timeline (`network.bin`).

use std::sync::Arc;

use gpui::{
    Bounds, Context, Div, FontWeight, MouseButton, MouseDownEvent, Pixels, ScrollStrategy,
    SharedString, UniformListScrollHandle, canvas, div, fill, point, prelude::*, px, rgb, rgba,
    size as gpui_size, uniform_list,
};

use crate::cluster::{
    ClusterData, ClusterEventFilter, NetEventTone, NodeSpanKind, format_virtual_ns,
};
use crate::describe::thousands;
use crate::theme::{self, layout, size};
use crate::ui::scrubber::Scrubber;
use crate::ui::widgets::{PillTone, pill};
use rewind_trace::cluster::ClusterVerdict;

/// Height of one node swimlane row on the cluster timeline canvas.
const LANE_ROW_H: f32 = 22.0;
const LANE_LABEL_W: f32 = 84.0;
const LANE_BAR_H: f32 = 12.0;
const EVENT_ROW_H: f32 = 24.0;

/// UI state for the Cluster tab inside `Scrubber`.
pub struct ClusterPanel {
    pub filter: ClusterEventFilter,
    pub scroll: UniformListScrollHandle,
    pub swimlane_bounds: std::rc::Rc<std::cell::Cell<Option<Bounds<Pixels>>>>,
}

impl Default for ClusterPanel {
    fn default() -> Self {
        ClusterPanel {
            filter: ClusterEventFilter::All,
            scroll: UniformListScrollHandle::new(),
            swimlane_bounds: std::rc::Rc::new(std::cell::Cell::new(None)),
        }
    }
}

impl Scrubber {
    /// Current cluster virtual timestamp corresponding to the active node's
    /// scrubber playhead.
    pub(super) fn current_cluster_ns(&self) -> Option<u64> {
        let session = self.session.as_ref()?;
        let cluster = session.cluster.as_ref()?;
        let node_idx = session.cluster_node?;
        Some(cluster.ns_for_step(node_idx, self.step, &session.run.timeline.trace))
    }

    /// Switches the active scrubber node inside the currently open cluster,
    /// keeping the playhead synchronized to `target_ns` (or the current
    /// cluster virtual time).
    pub(super) fn switch_cluster_node(
        &mut self,
        node_idx: usize,
        target_ns: Option<u64>,
        cx: &mut Context<Self>,
    ) {
        let Some(session) = self.session.as_ref() else {
            return;
        };
        let Some(cluster) = session.cluster.clone() else {
            return;
        };
        let Some(node) = cluster.nodes.get(node_idx) else {
            return;
        };
        if session.cluster_node == Some(node_idx) {
            if let Some(ns) = target_ns {
                let step = cluster.step_for_ns(node_idx, ns, &session.run.timeline.trace);
                self.jump_to(step, cx);
            }
            return;
        }
        let sync_ns = target_ns.or_else(|| self.current_cluster_ns());
        let run_dir = node.run_dir.clone();
        let request = self.requests.issue();
        self.opening = Some(request);
        self.loading = Some(run_dir.clone());
        cx.notify();

        let cluster_for_bg = cluster.clone();
        let read = cx.background_executor().spawn(async move {
            crate::run::Session::open_cluster_node(cluster_for_bg, node_idx)
        });
        cx.spawn(async move |this, cx| {
            let result = read.await;
            let _ = this.update(cx, |this, cx| {
                if this.opening != Some(request) {
                    return;
                }
                this.opening = None;
                this.loading = None;
                match result {
                    Ok(session) => {
                        let step = sync_ns.map(|ns| {
                            cluster.step_for_ns(node_idx, ns, &session.run.timeline.trace)
                        });
                        this.show(session, step, cx);
                    }
                    Err(e) => {
                        this.notify_user(
                            crate::ui::scrubber::NoticeTone::Error,
                            "Could not open cluster node",
                            format!("{}: {e:#}", run_dir.display()),
                            cx,
                        );
                    }
                }
            });
        })
        .detach();
    }

    /// Jumps the active node's playhead to cluster virtual time `ns`.
    pub(super) fn jump_to_cluster_ns(&mut self, ns: u64, cx: &mut Context<Self>) {
        let Some(session) = self.session.as_ref() else {
            return;
        };
        let Some(cluster) = session.cluster.as_ref() else {
            return;
        };
        let Some(node_idx) = session.cluster_node else {
            return;
        };
        let step = cluster.step_for_ns(node_idx, ns, &session.run.timeline.trace);
        self.jump_to(step, cx);
    }

    /// Compact cluster switcher strip rendered right under the header when the
    /// open run belongs to a multi-node cluster.
    pub(super) fn render_cluster_bar(&self, cx: &mut Context<Self>) -> Option<Div> {
        let session = self.session.as_ref()?;
        let cluster = session.cluster.as_ref()?;
        let active_idx = session.cluster_node;
        let now_ns = self.current_cluster_ns().unwrap_or(0);
        let partitioned = cluster.partitioned_at(now_ns);

        let verdict_pill = match &cluster.manifest.verdict {
            ClusterVerdict::Passed => pill("passed", PillTone::Passed, &self.fonts),
            ClusterVerdict::Failed { .. } => pill("failed", PillTone::Failed, &self.fonts),
            ClusterVerdict::Unfinished => pill("unfinished", PillTone::Running, &self.fonts),
        };

        let mut bar = div()
            .flex_none()
            .flex()
            .items_center()
            .gap(px(8.0))
            .px(px(size::PAGE_PAD_X))
            .py(px(6.0))
            .bg(rgb(theme::PANEL))
            .border_b_1()
            .border_color(rgb(theme::LINE))
            .child(
                div()
                    .font_family(self.fonts.mono.clone())
                    .text_size(px(size::TEXT_SMALL))
                    .font_weight(FontWeight::BOLD)
                    .text_color(rgb(theme::AMBER))
                    .child("CLUSTER"),
            )
            .child(
                div()
                    .text_size(px(size::TEXT_UI))
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(rgb(theme::TEXT))
                    .child(cluster.manifest.name.clone()),
            )
            .child(verdict_pill)
            .child(
                div()
                    .font_family(self.fonts.mono.clone())
                    .text_size(px(size::TEXT_SMALL))
                    .text_color(rgb(theme::MUTED))
                    .child(format!(
                        "{} / {}",
                        format_virtual_ns(now_ns),
                        format_virtual_ns(cluster.total_ns)
                    )),
            );

        if partitioned {
            bar = bar.child(pill("PARTITIONED", PillTone::Compared, &self.fonts));
        }

        // Clickable node pills with live state dot at current virtual time.
        let mut nodes_row = div().flex().items_center().gap(px(6.0)).ml(px(8.0));
        for node in &cluster.nodes {
            let idx = node.idx;
            let active = active_idx == Some(idx);
            let state = cluster.node_state_at(idx, now_ns);
            let dot_color = match state {
                NodeSpanKind::Running => theme::GREEN_SOFT,
                NodeSpanKind::Crashed => theme::RED,
                NodeSpanKind::Paused => theme::AMBER,
                NodeSpanKind::Clogged => theme::BLUE,
            };
            let bg = if active {
                theme::AMBER_CARD
            } else {
                theme::RAISED
            };
            let border = if active {
                theme::AMBER
            } else {
                theme::LINE_2
            };
            let label = match &node.zone {
                Some(z) => format!("{} ({z})", node.name),
                None => node.name.clone(),
            };
            let boots = (node.boot_count > 1).then(|| format!("b{}", node.boot_count));
            nodes_row = nodes_row.child(
                div()
                    .id(SharedString::from(format!("cluster-node-{idx}")))
                    .flex()
                    .items_center()
                    .gap(px(5.0))
                    .px(px(8.0))
                    .py(px(3.0))
                    .rounded(px(size::RADIUS_BUTTON))
                    .bg(rgb(bg))
                    .border_1()
                    .border_color(rgb(border))
                    .cursor_pointer()
                    .hover(|s| s.bg(rgb(if active { theme::AMBER_CARD } else { theme::RAISED_HOVER })))
                    .child(
                        div()
                            .w(px(7.0))
                            .h(px(7.0))
                            .rounded(px(4.0))
                            .bg(rgb(dot_color)),
                    )
                    .child(
                        div()
                            .font_family(self.fonts.mono.clone())
                            .text_size(px(size::TEXT_SMALL))
                            .font_weight(if active {
                                FontWeight::SEMIBOLD
                            } else {
                                FontWeight::NORMAL
                            })
                            .text_color(rgb(if active {
                                theme::AMBER_PALE
                            } else {
                                theme::TEXT
                            }))
                            .child(label),
                    )
                    .when_some(boots, |d, b| {
                        d.child(
                            div()
                                .font_family(self.fonts.mono.clone())
                                .text_size(px(10.0))
                                .text_color(rgb(theme::MUTED))
                                .child(b),
                        )
                    })
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.switch_cluster_node(idx, None, cx);
                    })),
            );
        }

        bar = bar.child(nodes_row);
        Some(bar)
    }

    /// Renders the Cluster right-hand tab: synced multi-node swimlanes +
    /// filterable virtual-scrolled L2 packet & fault event stream.
    pub(super) fn render_cluster_tab(&mut self, cx: &mut Context<Self>) -> Option<Div> {
        let session = self.session.as_ref()?;
        let cluster: Arc<ClusterData> = session.cluster.clone()?;
        let active_idx = session.cluster_node;
        let now_ns = self.current_cluster_ns().unwrap_or(0);
        let filter = self.cluster_panel.filter;
        let filtered = Arc::new(cluster.filtered_indices(filter, active_idx));

        // 1. Summary stats strip
        let stats_text = match &cluster.manifest.outcome {
            Some(o) => format!(
                "{} nodes · {} VT · {}ms wall · {} tx · {} dropped · {} partitions · {} crashes · {} restarts",
                cluster.nodes.len(),
                format_virtual_ns(cluster.total_ns),
                thousands(o.wall_ms),
                thousands(o.tx_frames),
                thousands(o.dropped_frames),
                o.partitions,
                o.crashes,
                o.restarts,
            ),
            None => format!(
                "{} nodes · {} events",
                cluster.nodes.len(),
                thousands(cluster.events.len() as u64)
            ),
        };

        let probes_line = (!cluster.manifest.probes.is_empty()
            || !cluster.manifest.buggify_sites.is_empty())
        .then(|| {
            let probes = cluster.manifest.probes.len();
            let active_buggify = cluster
                .manifest
                .buggify_sites
                .values()
                .filter(|&&v| v)
                .count();
            let total_buggify = cluster.manifest.buggify_sites.len();
            format!("probes: {probes} · buggify: {active_buggify}/{total_buggify} active")
        });

        // 2. Interactive synced swimlane canvas (one lane per node)
        let swimlane = self.render_cluster_swimlanes(cluster.clone(), active_idx, now_ns, cx);

        // 3. Event filter strip
        let active_label = active_idx
            .map(|i| cluster.node_name(i).to_string())
            .unwrap_or_else(|| "Node".into());
        let filters = [
            (
                ClusterEventFilter::All,
                format!("All ({})", thousands(cluster.visible_indices.len() as u64)),
            ),
            (
                ClusterEventFilter::Faults,
                format!("Faults ({})", cluster.fault_indices.len()),
            ),
            (
                ClusterEventFilter::Drops,
                format!("Drops + Faults ({})", cluster.drop_indices.len()),
            ),
            (ClusterEventFilter::ActiveNode, active_label),
        ];
        let mut filter_bar = div()
            .flex_none()
            .flex()
            .items_center()
            .gap(px(6.0))
            .px(px(12.0))
            .py(px(6.0))
            .border_b_1()
            .border_color(rgb(theme::LINE_SOFT));
        for (kind, label) in filters {
            let selected = filter == kind;
            filter_bar = filter_bar.child(
                div()
                    .id(SharedString::from(format!("cluster-filter-{kind:?}")))
                    .px(px(8.0))
                    .py(px(2.0))
                    .rounded(px(size::RADIUS_BUTTON))
                    .bg(rgb(if selected {
                        theme::AMBER_CARD
                    } else {
                        theme::RAISED
                    }))
                    .border_1()
                    .border_color(rgb(if selected {
                        theme::AMBER
                    } else {
                        theme::LINE_2
                    }))
                    .font_family(self.fonts.mono.clone())
                    .text_size(px(size::TEXT_SMALL))
                    .text_color(rgb(if selected {
                        theme::AMBER_PALE
                    } else {
                        theme::SOFT
                    }))
                    .cursor_pointer()
                    .child(label)
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.cluster_panel.filter = kind;
                        this.cluster_panel
                            .scroll
                            .scroll_to_item(0, ScrollStrategy::Top);
                        cx.notify();
                    })),
            );
        }

        // 4. Virtual-scrolled network + fault event list
        let item_count = filtered.len();
        let cluster_for_list = cluster.clone();
        let filtered_for_list = filtered.clone();
        let mono_font = self.fonts.mono.clone();
        let list = uniform_list(
            "cluster-events",
            item_count,
            cx.processor(move |_this, range: std::ops::Range<usize>, _window, cx| {
                let mut rows = Vec::with_capacity(range.len());
                for row_i in range {
                    let ev_idx = filtered_for_list[row_i];
                    let ev = &cluster_for_list.events[ev_idx];
                    let fmt = cluster_for_list.format_event(ev);
                    let near_now = fmt.ns.abs_diff(now_ns) <= 2_000_000;
                    let (badge_bg, badge_fg) = match fmt.tone {
                        NetEventTone::Packet => (theme::RAISED, theme::MUTED),
                        NetEventTone::Drop => (theme::RED_PILL, theme::RED_SOFT),
                        NetEventTone::FaultRed => (theme::RED_PILL, theme::RED),
                        NetEventTone::FaultAmber => (theme::AMBER_CARD, theme::AMBER),
                        NetEventTone::FaultBlue => (theme::BLUE_PILL, theme::BLUE_SOFT),
                        NetEventTone::FaultGreen => (theme::GREEN_PILL, theme::GREEN_SOFT),
                    };
                    let row_bg = if near_now {
                        theme::ROW_NOW
                    } else {
                        theme::PANEL
                    };
                    let target_ns = fmt.ns;
                    let target_node = fmt.node_idx;
                    let switch_on_click = ev_is_node_fault(ev);
                    rows.push(
                        div()
                            .id(SharedString::from(format!("net-ev-{row_i}")))
                            .h(px(EVENT_ROW_H))
                            .flex()
                            .items_center()
                            .gap(px(8.0))
                            .px(px(12.0))
                            .bg(rgb(row_bg))
                            .hover(|s| s.bg(rgb(theme::ROW_HOVER)))
                            .cursor_pointer()
                            .child(
                                div()
                                    .flex_none()
                                    .w(px(70.0))
                                    .font_family(mono_font.clone())
                                    .text_size(px(size::TEXT_SMALL))
                                    .text_color(rgb(theme::FAINT))
                                    .child(format_virtual_ns(fmt.ns)),
                            )
                            .child(
                                div()
                                    .flex_none()
                                    .w(px(68.0))
                                    .px(px(4.0))
                                    .py(px(1.0))
                                    .rounded(px(3.0))
                                    .bg(rgb(badge_bg))
                                    .font_family(mono_font.clone())
                                    .text_size(px(10.0))
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(rgb(badge_fg))
                                    .child(fmt.badge),
                            )
                            .child(
                                div()
                                    .flex_grow(layout::FILL)
                                    .min_w_0()
                                    .overflow_hidden()
                                    .whitespace_nowrap()
                                    .text_ellipsis()
                                    .font_family(mono_font.clone())
                                    .text_size(px(size::TEXT_SMALL))
                                    .text_color(rgb(if ev.is_fault() {
                                        theme::TEXT
                                    } else {
                                        theme::SOFT
                                    }))
                                    .child(fmt.summary),
                            )
                            .on_click(cx.listener(move |this, _, _, cx| {
                                if switch_on_click
                                    && let Some(n_idx) = target_node
                                    && this.session.as_ref().and_then(|s| s.cluster_node)
                                        != Some(n_idx)
                                {
                                    this.switch_cluster_node(n_idx, Some(target_ns), cx);
                                } else {
                                    this.jump_to_cluster_ns(target_ns, cx);
                                }
                            })),
                    );
                }
                rows
            }),
        )
        .track_scroll(&self.cluster_panel.scroll)
        .flex_grow(layout::FILL)
        .min_h_0();

        Some(
            div()
                .flex()
                .flex_col()
                .size_full()
                .bg(rgb(theme::PANEL))
                .child(
                    div()
                        .flex_none()
                        .flex()
                        .flex_col()
                        .gap(px(2.0))
                        .px(px(12.0))
                        .py(px(8.0))
                        .border_b_1()
                        .border_color(rgb(theme::LINE_SOFT))
                        .child(
                            div()
                                .font_family(self.fonts.mono.clone())
                                .text_size(px(size::TEXT_SMALL))
                                .text_color(rgb(theme::SOFT))
                                .child(stats_text),
                        )
                        .when_some(probes_line, |d, line| {
                            d.child(
                                div()
                                    .font_family(self.fonts.mono.clone())
                                    .text_size(px(size::TEXT_SMALL))
                                    .text_color(rgb(theme::AMBER_PALE))
                                    .child(line),
                            )
                        }),
                )
                .child(swimlane)
                .child(filter_bar)
                .child(list),
        )
    }

    fn render_cluster_swimlanes(
        &self,
        cluster: Arc<ClusterData>,
        active_idx: Option<usize>,
        now_ns: u64,
        cx: &mut Context<Self>,
    ) -> Div {
        let lane_count = cluster.nodes.len().max(1);
        let canvas_h = (lane_count as f32) * LANE_ROW_H + 12.0;
        let bounds_cell = self.cluster_panel.swimlane_bounds.clone();
        let bounds_for_click = bounds_cell.clone();
        let total_ns = cluster.total_ns.max(1);
        let cluster_for_paint = cluster.clone();

        let track = div()
            .id("cluster-swimlane-track")
            .flex_grow(layout::FILL)
            .h(px(canvas_h))
            .cursor_pointer()
            .child(
                canvas(
                    move |bounds, _window, _cx| {
                        bounds_cell.set(Some(bounds));
                    },
                    move |bounds, (), window, _cx| {
                        let width = f32::from(bounds.size.width).max(1.0);
                        let x_of = |ns: u64| -> f32 {
                            let f = (ns.min(total_ns) as f64 / total_ns as f64) as f32;
                            f32::from(bounds.origin.x) + f * width
                        };

                        // Partition intervals drawn behind all lanes.
                        for &(s, e) in &cluster_for_paint.partition_spans {
                            let x0 = x_of(s);
                            let x1 = x_of(e).max(x0 + 2.0);
                            window.paint_quad(fill(
                                Bounds::new(
                                    point(px(x0), bounds.origin.y),
                                    gpui_size(px(x1 - x0), bounds.size.height),
                                ),
                                rgba(0x7fb2ff24),
                            ));
                        }

                        // Per-node state spans.
                        for (idx, spans) in cluster_for_paint.spans.iter().enumerate() {
                            let y = f32::from(bounds.origin.y)
                                + 6.0
                                + (idx as f32) * LANE_ROW_H
                                + (LANE_ROW_H - LANE_BAR_H) / 2.0;
                            for span in spans {
                                let x0 = x_of(span.start_ns);
                                let x1 = x_of(span.end_ns).max(x0 + 2.0);
                                let color = match span.kind {
                                    NodeSpanKind::Running => {
                                        if active_idx == Some(idx) {
                                            0x39414f
                                        } else {
                                            0x262a31
                                        }
                                    }
                                    NodeSpanKind::Crashed => theme::RED,
                                    NodeSpanKind::Paused => theme::AMBER,
                                    NodeSpanKind::Clogged => theme::BLUE,
                                };
                                window.paint_quad(fill(
                                    Bounds::new(
                                        point(px(x0), px(y)),
                                        gpui_size(px(x1 - x0), px(LANE_BAR_H)),
                                    ),
                                    rgb(color),
                                ));
                            }
                        }

                        // Synced playhead vertical rule.
                        let px_now = x_of(now_ns);
                        window.paint_quad(fill(
                            Bounds::new(
                                point(px(px_now - 1.0), bounds.origin.y),
                                gpui_size(px(2.0), bounds.size.height),
                            ),
                            rgb(theme::AMBER),
                        ));
                    },
                )
                .size_full(),
            )
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, ev: &MouseDownEvent, _window, cx| {
                    let Some(bounds) = bounds_for_click.get() else {
                        return;
                    };
                    let width = f32::from(bounds.size.width).max(1.0);
                    let rel = ((f32::from(ev.position.x) - f32::from(bounds.origin.x)) / width)
                        .clamp(0.0, 1.0);
                    let target_ns = (rel as f64 * total_ns as f64).round() as u64;
                    this.jump_to_cluster_ns(target_ns, cx);
                }),
            );

        let mut labels = div()
            .flex_none()
            .w(px(LANE_LABEL_W))
            .pt(px(6.0))
            .flex()
            .flex_col();
        for node in &cluster.nodes {
            let idx = node.idx;
            let active = active_idx == Some(idx);
            labels = labels.child(
                div()
                    .id(SharedString::from(format!("lane-label-{idx}")))
                    .h(px(LANE_ROW_H))
                    .flex()
                    .items_center()
                    .px(px(6.0))
                    .font_family(self.fonts.mono.clone())
                    .text_size(px(size::TEXT_SMALL))
                    .font_weight(if active {
                        FontWeight::SEMIBOLD
                    } else {
                        FontWeight::NORMAL
                    })
                    .text_color(rgb(if active {
                        theme::AMBER_PALE
                    } else {
                        theme::MUTED
                    }))
                    .cursor_pointer()
                    .child(node.name.clone())
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.switch_cluster_node(idx, None, cx);
                    })),
            );
        }

        div()
            .flex_none()
            .flex()
            .items_start()
            .px(px(6.0))
            .py(px(4.0))
            .bg(rgb(theme::BG))
            .border_b_1()
            .border_color(rgb(theme::LINE))
            .child(labels)
            .child(track)
    }
}

fn ev_is_node_fault(ev: &rewind_trace::net_log::NetEvent) -> bool {
    matches!(
        ev,
        rewind_trace::net_log::NetEvent::NodeCrash { .. }
            | rewind_trace::net_log::NetEvent::NodeRestart { .. }
            | rewind_trace::net_log::NetEvent::NodePause { .. }
            | rewind_trace::net_log::NetEvent::NodeResume { .. }
            | rewind_trace::net_log::NetEvent::ClockStep { .. }
            | rewind_trace::net_log::NetEvent::ClockDrift { .. }
    )
}
