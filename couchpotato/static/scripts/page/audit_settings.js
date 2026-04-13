var AuditSettingTab = new Class({

	container: null,
	scan_toolbar: null,
	progress_container: null,
	summary_container: null,
	items_container: null,
	batch_bar: null,
	preview_modal: null,
	_expand_next: false,

	tier1_btn: null,
	tier2_btn: null,
	cancel_btn: null,

	scan_in_progress: false,
	progress_interval: null,
	progress_request: null,
	fix_progress_interval: null,

	// Pagination state
	current_offset: 0,
	current_limit: 50,
	current_filter_check: '',
	current_filter_severity: '',
	current_filter_action: '',
	current_filter_fixed: 'false',
	current_sort: 'folder',
	current_sort_dir: 'asc',
	total_filtered: 0,

	// Loaded items cache
	loaded_items: [],

	initialize: function(){
		var self = this;
		App.addEvent('loadSettings', self.addSettings.bind(self));
	},

	addSettings: function(){
		var self = this;

		self.settings = App.getPage('Settings');
		self.settings.addEvent('create', function(){
			requestTimeout(function(){
				var tab = self.settings.tabs['manage'];
				if(!tab) return;

				var subtab = tab.subtabs['audit'];
				if(!subtab) return;

				self.container = subtab.content;
				self.buildUI();

				// Check if scan is in progress or results exist
				self.checkInitialState();
			}, 150);
		});
	},

	// -----------------------------------------------------------------------
	// UI Construction
	// -----------------------------------------------------------------------

	buildUI: function(){
		var self = this;

		// Create a settings-style group wrapper
		var group = self.settings.createGroup({
			'label': 'Library Audit',
			'name': 'audit_main'
		}).inject(self.container, 'top');

		self.audit_wrap = new Element('div.audit_wrap').inject(group);

		self.createScanControls();
		self.createProgressSection();
		self.createSummaryDashboard();
		self.createItemsList();
		self.createBatchBar();
		self.createPreviewModal();
	},

	createScanControls: function(){
		var self = this;

		// Reuse .scan_toolbar wrapper so .scan_btn etc. inherit existing styles
		self.scan_toolbar = new Element('div.scan_toolbar').inject(self.audit_wrap);

		var buttons_row = new Element('div.scan_buttons').inject(self.scan_toolbar);

		self.tier1_btn = new Element('a.scan_btn.audit_tier1', {
			'events': { 'click': self.startScan.bind(self, false) }
		}).adopt(
			new Element('span.scan_btn_icon.icon-search'),
			new Element('span.scan_btn_label', { 'text': 'Tier 1 Scan' }),
			new Element('span.scan_btn_desc', { 'text': 'Quick local scan: resolution, runtime, title, TV episodes, editions' })
		).inject(buttons_row);

		self.tier2_btn = new Element('a.scan_btn.audit_tier2', {
			'events': { 'click': self.startScan.bind(self, true) }
		}).adopt(
			new Element('span.scan_btn_icon.icon-refresh'),
			new Element('span.scan_btn_label', { 'text': 'Tier 1 + 2 Scan' }),
			new Element('span.scan_btn_desc', { 'text': 'Full scan with CRC/srrDB identification (~20 min)' })
		).inject(buttons_row);

		self.cancel_btn = new Element('a.scan_btn.audit_cancel', {
			'events': { 'click': self.cancelScan.bind(self) }
		}).adopt(
			new Element('span.scan_btn_icon.icon-cancel'),
			new Element('span.scan_btn_label', { 'text': 'Cancel' })
		).inject(buttons_row);
		self.cancel_btn.setStyle('display', 'none');
	},

	createProgressSection: function(){
		var self = this;

		// Reuse .scan_progress inside .scan_toolbar for existing progress styles
		self.progress_container = new Element('div.scan_progress').inject(self.scan_toolbar);
		self.progress_container.setStyle('display', 'none');
	},

	createSummaryDashboard: function(){
		var self = this;

		// Reuse .metadata_stats for the dark panel + stat styles
		self.summary_container = new Element('div.metadata_stats').inject(self.audit_wrap);
		self.summary_container.setStyle('display', 'none');
	},

	createItemsList: function(){
		var self = this;

		// Filters bar
		var filters = new Element('div.audit_filters').inject(self.audit_wrap);
		filters.setStyle('display', 'none');
		self.filters_container = filters;

		// Check type filter
		new Element('label.audit_filter_label', { 'text': 'Type:' }).inject(filters);
		self.filter_check_select = new Element('select.audit_filter_select', {
			'events': { 'change': self.onFilterChange.bind(self) }
		}	).adopt(
			new Element('option', { 'value': '', 'text': 'All' }),
			new Element('option', { 'value': 'resolution', 'text': 'Resolution' }),
			new Element('option', { 'value': 'runtime', 'text': 'Runtime' }),
			new Element('option', { 'value': 'title', 'text': 'Title' }),
			new Element('option', { 'value': 'tv_episode', 'text': 'TV Episode' }),
			new Element('option', { 'value': 'edition', 'text': 'Edition' }),
			new Element('option', { 'value': 'template', 'text': 'Template' })
		).inject(filters);

		// Severity filter
		new Element('label.audit_filter_label', { 'text': 'Severity:' }).inject(filters);
		self.filter_severity_select = new Element('select.audit_filter_select', {
			'events': { 'change': self.onFilterChange.bind(self) }
		}).adopt(
			new Element('option', { 'value': '', 'text': 'All' }),
			new Element('option', { 'value': 'HIGH', 'text': 'High' }),
			new Element('option', { 'value': 'MEDIUM', 'text': 'Medium' }),
			new Element('option', { 'value': 'LOW', 'text': 'Low' })
		).inject(filters);

		// Action filter
		new Element('label.audit_filter_label', { 'text': 'Action:' }).inject(filters);
		self.filter_action_select = new Element('select.audit_filter_select', {
			'events': { 'change': self.onFilterChange.bind(self) }
		}		).adopt(
			new Element('option', { 'value': '', 'text': 'All' }),
			new Element('option', { 'value': 'rename_template', 'text': 'Rename to Template' }),
			new Element('option', { 'value': 'rename_resolution', 'text': 'Rename Resolution' }),
			new Element('option', { 'value': 'rename_edition', 'text': 'Rename Edition' }),
			new Element('option', { 'value': 'delete_wrong', 'text': 'Delete Wrong' }),
			new Element('option', { 'value': 'reassign_movie', 'text': 'Reassign Movie' }),
			new Element('option', { 'value': 'needs_tier2', 'text': 'Needs Tier 2' }),
			new Element('option', { 'value': 'manual_review', 'text': 'Manual Review' })
		).inject(filters);

		// Sort
		new Element('label.audit_filter_label', { 'text': 'Sort:' }).inject(filters);
		self.sort_select = new Element('select.audit_filter_select', {
			'events': { 'change': self.onFilterChange.bind(self) }
		}).adopt(
			new Element('option', { 'value': 'folder', 'text': 'Folder A-Z' }),
			new Element('option', { 'value': 'severity', 'text': 'Severity' }),
			new Element('option', { 'value': 'flag_count', 'text': 'Flag Count' }),
			new Element('option', { 'value': 'file_size', 'text': 'File Size' })
		).inject(filters);

		// Show fixed toggle
		self.filter_fixed_check = new Element('input.audit_filter_checkbox', {
			'type': 'checkbox',
			'events': { 'change': self.onFilterChange.bind(self) }
		});
		new Element('label.audit_filter_label.audit_filter_fixed').adopt(
			self.filter_fixed_check,
			new Element('span', { 'text': ' Show fixed' })
		).inject(filters);

		// Items container
		self.items_container = new Element('div.audit_items').inject(self.audit_wrap);
		self.items_container.setStyle('display', 'none');

		// Pagination
		self.pagination_container = new Element('div.audit_pagination').inject(self.audit_wrap);
		self.pagination_container.setStyle('display', 'none');
	},

	createBatchBar: function(){
		var self = this;

		self.batch_bar = new Element('div.audit_batch_bar').inject(self.audit_wrap);
		self.batch_bar.setStyle('display', 'none');
	},

	createPreviewModal: function(){
		var self = this;

		self.preview_modal = new Element('div.audit_modal_overlay').inject(document.body);
		self.preview_modal.setStyle('display', 'none');

		self.preview_modal.addEvent('click', function(e){
			if(e.target === self.preview_modal)
				self.closePreviewModal();
		});
	},

	// -----------------------------------------------------------------------
	// Initial State Check
	// -----------------------------------------------------------------------

	checkInitialState: function(){
		var self = this;

		// Check if scan is running
		Api.request('audit.progress', {
			'onComplete': function(json){
				if(json && json.progress){
					self.scan_in_progress = true;
					self.setButtonsScanning(true);
					self.startProgressPolling();
				} else {
					// Check if we have existing results
					self.loadStats();
				}
			}
		});
	},

	// -----------------------------------------------------------------------
	// Scan Controls
	// -----------------------------------------------------------------------

	startScan: function(tier2){
		var self = this;
		if(self.scan_in_progress) return;

		var data = { 'tier2': tier2 ? 1 : 0, 'workers': 4 };

		Api.request('audit.scan', {
			'data': data,
			'onComplete': function(json){
				if(json && json.success !== false){
					self.scan_in_progress = true;
					self.setButtonsScanning(true);
					self.summary_container.setStyle('display', 'none');
					self.items_container.setStyle('display', 'none');
					self.filters_container.setStyle('display', 'none');
					self.pagination_container.setStyle('display', 'none');
					self.batch_bar.setStyle('display', 'none');
					self.startProgressPolling();
				}
			}
		});
	},

	cancelScan: function(){
		var self = this;
		Api.request('audit.cancel', {
			'onComplete': function(){
				// Progress polling will detect scan end
			}
		});
	},

	setButtonsScanning: function(scanning){
		var self = this;
		if(scanning){
			self.tier1_btn.addClass('disabled');
			self.tier2_btn.addClass('disabled');
			self.cancel_btn.setStyle('display', '');
		} else {
			self.tier1_btn.removeClass('disabled');
			self.tier2_btn.removeClass('disabled');
			self.cancel_btn.setStyle('display', 'none');
		}
	},

	// -----------------------------------------------------------------------
	// Progress Polling
	// -----------------------------------------------------------------------

	startProgressPolling: function(){
		var self = this;

		if(self.progress_interval)
			clearRequestInterval(self.progress_interval);

		self.progress_container.setStyle('display', '');

		self.progress_interval = requestInterval(function(){
			if(self.progress_request && self.progress_request.running)
				return;

			self.progress_request = Api.request('audit.progress', {
				'onComplete': function(json){
					if(!json || !json.progress){
						// Scan finished
						clearRequestInterval(self.progress_interval);
						self.progress_interval = null;
						self.scan_in_progress = false;
						self.setButtonsScanning(false);
						self.progress_container.setStyle('display', 'none');
						self.progress_container.empty();

						// Load fresh stats and results
						self.loadStats();
						return;
					}

					var p = json.progress;
					self.progress_container.empty();

					var pct = p.total > 0 ? Math.round((p.scanned / p.total) * 100) : 0;

					// Status line — reuse .scan_status / .scan_status_icon / .scan_live_count
					new Element('div.scan_status').adopt(
						new Element('span.scan_status_icon.icon-refresh.spinning'),
						new Element('span', { 'text': 'Scanning library... ' }),
						new Element('span.scan_live_count', {
							'text': p.scanned + ' / ' + p.total + ' (' + pct + '%) \u2014 ' + p.flagged + ' flagged'
						})
					).inject(self.progress_container);

					// Progress bar — reuse .stats_bar_wrap / .stats_bar / .stats_bar_text
					// Wrap in .metadata_stats so the existing selectors match
					var bar_host = new Element('div.metadata_stats.audit_progress_bar_host').inject(self.progress_container);
					var bar_wrap = new Element('div.stats_bar_wrap').inject(bar_host);
					new Element('div.stats_bar.bar_ok', {
						'styles': { 'width': pct + '%' }
					}).inject(bar_wrap);
					new Element('div.stats_bar_text', {
						'text': pct + '%'
					}).inject(bar_wrap);
				}
			});
		}, 1000);
	},

	// -----------------------------------------------------------------------
	// Stats / Summary Dashboard
	// -----------------------------------------------------------------------

	loadStats: function(){
		var self = this;

		Api.request('audit.stats', {
			'onComplete': function(json){
				if(!json || !json.stats){
					self.summary_container.setStyle('display', 'none');
					return;
				}

				self.renderSummary(json.stats);
				self.loadResults();
			}
		});
	},

	renderSummary: function(stats){
		var self = this;

		self.summary_container.empty();
		self.summary_container.setStyle('display', '');

		// Stats grid row — reuse .stats_grid / .stat_item / .stat_value / .stat_label
		var grid = new Element('div.stats_grid').inject(self.summary_container);

		var stat_items = [
			{ label: 'Scanned', value: stats.total_scanned || 0 },
			{ label: 'Flagged', value: stats.total_flagged || 0 },
			{ label: 'Clean', value: stats.total_clean || 0 },
			{ label: 'Fixed', value: stats.total_fixed || 0 },
			{ label: 'Ignored', value: stats.total_ignored || 0 },
			{ label: 'Errors', value: stats.total_errors || 0 }
		];

		stat_items.each(function(item){
			new Element('div.stat_item').adopt(
				new Element('div.stat_value', { 'text': self.formatNumber(item.value) }),
				new Element('div.stat_label', { 'text': item.label })
			).inject(grid);
		});

		// Flag type bars
		if(stats.checks){
			var bars_section = new Element('div.audit_flag_bars').inject(self.summary_container);
			new Element('div.audit_bars_title', { 'text': 'Issues by Type' }).inject(bars_section);

			var max_count = 0;
			var check_types = ['resolution', 'title', 'runtime', 'tv_episode', 'edition', 'template'];
			var check_labels = {
				'resolution': 'Resolution Mismatch',
				'title': 'Title Mismatch',
				'runtime': 'Runtime Mismatch',
				'tv_episode': 'TV Episode',
				'edition': 'Edition Missing',
				'template': 'Template Mismatch'
			};
			var check_colors = {
				'resolution': '#ff9800',
				'title': '#f44336',
				'runtime': '#ff9800',
				'tv_episode': '#f44336',
				'edition': '#2196f3',
				'template': '#9c27b0'
			};

			check_types.each(function(ct){
				var c = (stats.checks[ct] || 0);
				if(c > max_count) max_count = c;
			});

			check_types.each(function(ct){
				var count = stats.checks[ct] || 0;
				if(count === 0) return;
				var pct = max_count > 0 ? Math.round((count / max_count) * 100) : 0;

				var row = new Element('div.audit_bar_row').inject(bars_section);
				new Element('div.audit_bar_label', { 'text': check_labels[ct] || ct }).inject(row);
				var bar_wrap = new Element('div.audit_bar_wrap').inject(row);
				new Element('div.audit_bar_fill', {
					'styles': { 'width': pct + '%', 'background': check_colors[ct] || '#999' }
				}).inject(bar_wrap);
				new Element('div.audit_bar_count', { 'text': self.formatNumber(count) }).inject(row);
			});
		}

		// Action breakdown
		if(stats.actions){
			var actions_section = new Element('div.audit_action_summary').inject(self.summary_container);
			new Element('div.audit_bars_title', { 'text': 'Recommended Actions' }).inject(actions_section);

			var action_labels = {
				'rename_template': 'Rename to Template',
				'rename_resolution': 'Rename Resolution',
				'rename_edition': 'Rename Edition',
				'delete_wrong': 'Delete TV Episodes',
				'reassign_movie': 'Reassign Movie',
				'needs_tier2': 'Needs Tier 2 ID',
				'manual_review': 'Manual Review'
			};
			var action_colors = {
				'rename_template': '#9c27b0',
				'rename_resolution': '#4caf50',
				'rename_edition': '#2196f3',
				'delete_wrong': '#f44336',
				'reassign_movie': '#ff9800',
				'needs_tier2': '#9e9e9e',
				'manual_review': '#9e9e9e'
			};

			var max_action = 0;
			Object.each(stats.actions, function(v){ if(v > max_action) max_action = v; });

			Object.each(stats.actions, function(count, action){
				if(count === 0) return;
				var pct = max_action > 0 ? Math.round((count / max_action) * 100) : 0;
				var row = new Element('div.audit_bar_row').inject(actions_section);
				new Element('div.audit_bar_label', { 'text': action_labels[action] || action }).inject(row);
				var bar_wrap = new Element('div.audit_bar_wrap').inject(row);
				new Element('div.audit_bar_fill', {
					'styles': { 'width': pct + '%', 'background': action_colors[action] || '#999' }
				}).inject(bar_wrap);
				new Element('div.audit_bar_count', { 'text': self.formatNumber(count) }).inject(row);
			});
		}
	},

	// -----------------------------------------------------------------------
	// Flagged Items List
	// -----------------------------------------------------------------------

	loadResults: function(){
		var self = this;

		var data = {
			'offset': self.current_offset,
			'limit': self.current_limit
		};
		if(self.current_filter_check) data.filter_check = self.current_filter_check;
		if(self.current_filter_severity) data.filter_severity = self.current_filter_severity;
		if(self.current_filter_action) data.filter_action = self.current_filter_action;
		data.filter_fixed = self.current_filter_fixed;
		data.sort = self.current_sort;
		data.sort_dir = self.current_sort_dir;

		Api.request('audit.results', {
			'data': data,
			'onComplete': function(json){
				if(!json || !json.results){
					self.items_container.setStyle('display', 'none');
					self.filters_container.setStyle('display', 'none');
					self.pagination_container.setStyle('display', 'none');
					self.batch_bar.setStyle('display', 'none');
					return;
				}

				var r = json.results;
				self.total_filtered = r.total_filtered || 0;
				self.loaded_items = r.items || [];

				self.filters_container.setStyle('display', '');
				self.renderItems(self.loaded_items);
				self.renderPagination();
				self.renderBatchBar();
			}
		});
	},

	onFilterChange: function(){
		var self = this;
		self.current_filter_check = self.filter_check_select.get('value');
		self.current_filter_severity = self.filter_severity_select.get('value');
		self.current_filter_action = self.filter_action_select.get('value');
		self.current_filter_fixed = self.filter_fixed_check.checked ? 'all' : 'false';

		var sort_val = self.sort_select.get('value');
		// Severity and flag_count sort descending by default
		if(sort_val === 'severity' || sort_val === 'flag_count' || sort_val === 'file_size'){
			self.current_sort = sort_val;
			self.current_sort_dir = 'desc';
		} else {
			self.current_sort = sort_val;
			self.current_sort_dir = 'asc';
		}

		self.current_offset = 0;
		self.loadResults();
	},

	renderItems: function(items){
		var self = this;

		self.items_container.empty();
		self.items_container.setStyle('display', '');

		if(!items || items.length === 0){
			new Element('div.audit_no_items', {
				'text': 'No flagged items match the current filters.'
			}).inject(self.items_container);
			return;
		}

		items.each(function(item){
			self.renderItem(item);
		});

		// Auto-expand the new top card if the previous top card was acted on
		if(self._expand_next){
			self._expand_next = false;
			var first = self.items_container.getFirst('.audit_item');
			if(first) first.addClass('expanded');
		}
	},

	renderItem: function(item){
		var self = this;

		var card = new Element('div.audit_item', {
			'data-item-id': item.item_id
		}).inject(self.items_container);
		if(item.fixed) card.addClass('audit_item_fixed');

		// Header (collapsed view)
		var header = new Element('div.audit_item_header', {
			'events': {
				'click': function(){
					card.toggleClass('expanded');
				}
			}
		}).inject(card);

		// Expand arrow
		new Element('span.audit_item_arrow.icon-right-dir').inject(header);

		// Folder name
		new Element('span.audit_item_folder', { 'text': item.folder || '' }).inject(header);

		// Severity badge
		var sev_order = {'LOW': 0, 'MEDIUM': 1, 'HIGH': 2};
		var max_severity = 'LOW';
		if(item.flags){
			item.flags.each(function(f){
				if((sev_order[f.severity] || 0) > (sev_order[max_severity] || 0))
					max_severity = f.severity;
			});
		}
		new Element('span.audit_severity_badge.audit_severity_' + max_severity.toLowerCase(), {
			'text': max_severity
		}).inject(header);

		// Flag type icons
		var flag_types = {};
		if(item.flags){
			item.flags.each(function(f){ flag_types[f.check] = true; });
		}
		var icons_wrap = new Element('span.audit_item_flags').inject(header);
		Object.each(flag_types, function(v, check){
			new Element('span.audit_flag_icon', {
				'text': self.flagIcon(check),
				'title': check
			}).inject(icons_wrap);
		});

		// Action badge
		if(item.recommended_action && item.recommended_action !== 'none'){
			new Element('span.audit_action_badge', {
				'text': self.actionLabel(item.recommended_action)
			}).inject(header);
		}

		// Fixed badge
		if(item.fixed){
			new Element('span.audit_fixed_badge', { 'text': 'FIXED' }).inject(header);
		}

		// Detail (expanded view)
		var detail = new Element('div.audit_item_detail').inject(card);

		// File info
		new Element('div.audit_detail_row').adopt(
			new Element('span.audit_detail_label', { 'text': 'File:' }),
			new Element('span.audit_detail_value.audit_file_path', { 'text': item.file_path || item.file || '' })
		).inject(detail);

		if(item.file_size_bytes){
			new Element('div.audit_detail_row').adopt(
				new Element('span.audit_detail_label', { 'text': 'Size:' }),
				new Element('span.audit_detail_value', { 'text': self.formatBytes(item.file_size_bytes) })
			).inject(detail);
		}

		// Flags detail
		if(item.flags && item.flags.length > 0){
			var flags_section = new Element('div.audit_detail_flags').inject(detail);
			new Element('div.audit_detail_subtitle', { 'text': 'Flags:' }).inject(flags_section);

			item.flags.each(function(flag){
				var flag_row = new Element('div.audit_flag_row').inject(flags_section);
				new Element('span.audit_flag_check', { 'text': flag.check }).inject(flag_row);
				new Element('span.audit_severity_badge.audit_severity_' + (flag.severity || 'medium').toLowerCase(), {
					'text': flag.severity
				}).inject(flag_row);
				new Element('span.audit_flag_detail', { 'text': flag.detail || '' }).inject(flag_row);
			});
		}

		// Actual vs Expected
		if(item.actual){
			var actual_section = new Element('div.audit_detail_section').inject(detail);
			new Element('div.audit_detail_subtitle', { 'text': 'Actual:' }).inject(actual_section);
			self.renderKeyValues(actual_section, item.actual);
		}
		if(item.expected){
			var expected_section = new Element('div.audit_detail_section').inject(detail);
			new Element('div.audit_detail_subtitle', { 'text': 'Expected:' }).inject(expected_section);
			self.renderKeyValues(expected_section, item.expected);
		}

		// Identification (tier 2)
		if(item.identification && item.identification.method && item.identification.method !== 'skipped'){
			var id_section = new Element('div.audit_detail_section').inject(detail);
			new Element('div.audit_detail_subtitle', { 'text': 'Identification:' }).inject(id_section);
			self.renderKeyValues(id_section, item.identification);
		}

		// Action buttons
		if(!item.fixed){
			var actions_row = new Element('div.audit_item_actions').inject(detail);
			var action = item.recommended_action;

			if(action && action !== 'none' && action !== 'needs_tier2' && action !== 'manual_review'){
				new Element('a.audit_action_btn.primary', {
					'text': self.actionLabel(action),
					'events': { 'click': function(e){
						e.stop();
						self.showFixPreview(item.item_id, action);
					}}
				}).inject(actions_row);
			}

			// Also show alternative actions
			var alt_actions = self.getAlternativeActions(item);
			alt_actions.each(function(alt){
				if(alt === action) return;
				new Element('a.audit_action_btn.secondary', {
					'text': self.actionLabel(alt),
					'events': { 'click': function(e){
						e.stop();
						self.showFixPreview(item.item_id, alt);
					}}
				}).inject(actions_row);
			});

			// Run Tier 2 button
			var t2_btn = new Element('a.audit_action_btn.audit_tier2_btn', {
				'text': 'Run Tier 2',
				'events': { 'click': function(e){
					e.stop();
					self.runTier2(item.item_id, t2_btn, card);
				}}
			}).inject(actions_row);

			// Delete button (per-card, not batch)
			new Element('a.audit_action_btn.audit_delete_btn', {
				'text': 'Delete',
				'events': { 'click': function(e){
					e.stop();
					self.showFixPreview(item.item_id, 'delete_wrong');
				}}
			}).inject(actions_row);

			// Ignore button (two-click confirm: first click arms, second click executes)
			var ignore_btn = new Element('a.audit_action_btn.audit_ignore_btn', {
				'text': 'Ignore',
				'events': { 'click': function(e){
					e.stop();
					if(ignore_btn.hasClass('confirming')){
						self.ignoreItem(item.item_id, card);
					} else {
						ignore_btn.addClass('confirming');
						ignore_btn.set('text', 'Confirm?');
						// Reset on any outside click
						var reset = function(ev){
							if(ev.target !== ignore_btn){
								ignore_btn.removeClass('confirming');
								ignore_btn.set('text', 'Ignore');
								document.body.removeEvent('click', reset);
							}
						};
						document.body.addEvent('click', reset);
					}
				}}
			}).inject(actions_row);

			// Manual IMDB reassignment row
			var reassign_row = new Element('div.audit_reassign_row').inject(detail);
			var imdb_input = new Element('input.audit_imdb_input', {
				'type': 'text',
				'placeholder': 'tt1234567',
				'events': {
					'click': function(e){ e.stop(); },
					'keydown': function(e){
						if(e.key === 'enter'){
							e.stop();
							self.manualReassign(item.item_id, imdb_input, reassign_btn);
						}
					}
				}
			}).inject(reassign_row);
			var reassign_btn = new Element('a.audit_action_btn.audit_reassign_btn', {
				'text': 'Reassign',
				'events': { 'click': function(e){
					e.stop();
					self.manualReassign(item.item_id, imdb_input, reassign_btn);
				}}
			}).inject(reassign_row);
		}
	},

	renderKeyValues: function(container, obj){
		Object.each(obj, function(val, key){
			if(val === null || val === undefined) return;
			if(typeof val === 'object'){
				val = JSON.stringify(val);
			}
			new Element('div.audit_kv_row').adopt(
				new Element('span.audit_kv_key', { 'text': key + ':' }),
				new Element('span.audit_kv_val', { 'text': String(val) })
			).inject(container);
		});
	},

	// -----------------------------------------------------------------------
	// Pagination
	// -----------------------------------------------------------------------

	renderPagination: function(){
		var self = this;

		self.pagination_container.empty();

		if(self.total_filtered <= self.current_limit){
			self.pagination_container.setStyle('display', 'none');
			return;
		}

		self.pagination_container.setStyle('display', '');

		var total_pages = Math.ceil(self.total_filtered / self.current_limit);
		var current_page = Math.floor(self.current_offset / self.current_limit) + 1;

		// Prev button
		if(current_page > 1){
			new Element('a.audit_page_btn', {
				'text': '\u2190 Prev',
				'events': { 'click': function(){
					self.current_offset = Math.max(0, self.current_offset - self.current_limit);
					self.loadResults();
				}}
			}).inject(self.pagination_container);
		}

		// Page info
		new Element('span.audit_page_info', {
			'text': 'Page ' + current_page + ' of ' + total_pages + ' (' + self.formatNumber(self.total_filtered) + ' items)'
		}).inject(self.pagination_container);

		// Next button
		if(current_page < total_pages){
			new Element('a.audit_page_btn', {
				'text': 'Next \u2192',
				'events': { 'click': function(){
					self.current_offset += self.current_limit;
					self.loadResults();
				}}
			}).inject(self.pagination_container);
		}
	},

	// -----------------------------------------------------------------------
	// Batch Actions Bar
	// -----------------------------------------------------------------------

	renderBatchBar: function(){
		var self = this;

		self.batch_bar.empty();

		// Only show if we have results
		if(self.total_filtered === 0){
			self.batch_bar.setStyle('display', 'none');
			return;
		}

		self.batch_bar.setStyle('display', '');

		new Element('span.audit_batch_label', { 'text': 'Batch Actions:' }).inject(self.batch_bar);

		var batch_actions = [
			{ action: 'rename_template', label: 'Rename All to Template' },
			{ action: 'rename_resolution', label: 'Rename All Resolution' },
			{ action: 'rename_edition', label: 'Rename All Edition' },
			{ action: 'delete_wrong', label: 'Delete All TV Episodes' }
		];

		batch_actions.each(function(ba){
			new Element('a.audit_batch_btn', {
				'text': ba.label,
				'events': { 'click': function(e){
					e.stop();
					self.startBatchFix(ba.action);
				}}
			}).inject(self.batch_bar);
		});
	},

	startBatchFix: function(action){
		var self = this;

		// First do a dry run to get counts
		var data = {
			'action': action,
			'confirm': 1,
			'dry_run': 1
		};
		if(self.current_filter_check) data.filter_check = self.current_filter_check;
		if(self.current_filter_severity) data.filter_severity = self.current_filter_severity;

		Api.request('audit.fix.batch', {
			'data': data,
			'onComplete': function(json){
				if(!json || json.success === false){
					alert('Batch preview failed: ' + (json ? json.error || 'Unknown error' : 'No response'));
					return;
				}

				var previews = json.previews || [];
				var count = previews.length;
				if(count === 0){
					alert('No items match this batch action.');
					return;
				}

				if(!confirm('This will apply "' + self.actionLabel(action) + '" to ' + count + ' items. Continue?')){
					return;
				}

				// Execute batch
				var exec_data = {
					'action': action,
					'confirm': 1,
					'dry_run': 0
				};
				if(self.current_filter_check) exec_data.filter_check = self.current_filter_check;
				if(self.current_filter_severity) exec_data.filter_severity = self.current_filter_severity;

				Api.request('audit.fix.batch', {
					'data': exec_data,
					'onComplete': function(json2){
						if(json2 && json2.success !== false){
							self.startFixProgressPolling();
						} else {
							alert('Batch execution failed: ' + (json2 ? json2.error || 'Unknown error' : 'No response'));
						}
					}
				});
			}
		});
	},

	startFixProgressPolling: function(){
		var self = this;

		if(self.fix_progress_interval)
			clearRequestInterval(self.fix_progress_interval);

		// Show progress in the batch bar
		self.batch_bar.empty();
		new Element('div.audit_batch_progress').adopt(
			new Element('span.icon-refresh.spinning'),
			new Element('span.audit_batch_progress_text', { 'text': ' Batch fix in progress...' })
		).inject(self.batch_bar);

		self.fix_progress_interval = requestInterval(function(){
			Api.request('audit.fix.progress', {
				'onComplete': function(json){
					if(!json || !json.fix_progress || !json.fix_progress.active){
						clearRequestInterval(self.fix_progress_interval);
						self.fix_progress_interval = null;

						// Refresh everything
						self.loadStats();
						return;
					}

					var fp = json.fix_progress;
					var text = ' Fixing: ' + fp.completed + ' / ' + fp.total;
					if(fp.failed > 0) text += ' (' + fp.failed + ' failed)';
					if(fp.current_item) text += ' \u2014 ' + fp.current_item;

					self.batch_bar.empty();
					new Element('div.audit_batch_progress').adopt(
						new Element('span.icon-refresh.spinning'),
						new Element('span.audit_batch_progress_text', { 'text': text })
					).inject(self.batch_bar);
				}
			});
		}, 1000);
	},

	// -----------------------------------------------------------------------
	// Fix Preview Modal
	// -----------------------------------------------------------------------

	showFixPreview: function(item_id, action){
		var self = this;

		Api.request('audit.fix.preview', {
			'data': { 'item_id': item_id, 'action': action },
			'onComplete': function(json){
				if(!json || json.success === false){
					alert('Preview failed: ' + (json ? json.error || 'Unknown error' : 'No response'));
					return;
				}

				self.renderPreviewModal(json.preview, item_id, action);
			}
		});
	},

	renderPreviewModal: function(preview, item_id, action){
		var self = this;

		self.preview_modal.empty();
		self.preview_modal.setStyle('display', '');

		var modal = new Element('div.audit_modal').inject(self.preview_modal);

		// Header
		new Element('div.audit_modal_header').adopt(
			new Element('h3', { 'text': 'Fix Preview: ' + self.actionLabel(action) }),
			new Element('a.audit_modal_close.icon-cancel', {
				'events': { 'click': self.closePreviewModal.bind(self) }
			})
		).inject(modal);

		var body = new Element('div.audit_modal_body').inject(modal);

		// Filesystem changes
		if(preview && preview.changes && preview.changes.filesystem){
			var fs = preview.changes.filesystem;
			var fs_section = new Element('div.audit_preview_section').inject(body);
			new Element('div.audit_preview_title', { 'text': 'Filesystem Changes' }).inject(fs_section);

			if(fs.old_path){
				new Element('div.audit_preview_row').adopt(
					new Element('span.audit_preview_label', { 'text': 'From:' }),
					new Element('span.audit_preview_old', { 'text': fs.old_path })
				).inject(fs_section);
			}
			if(fs.new_path){
				new Element('div.audit_preview_row').adopt(
					new Element('span.audit_preview_label', { 'text': 'To:' }),
					new Element('span.audit_preview_new', { 'text': fs.new_path })
				).inject(fs_section);
			}
			if(fs.delete_path){
				new Element('div.audit_preview_row').adopt(
					new Element('span.audit_preview_label', { 'text': 'Delete:' }),
					new Element('span.audit_preview_old', { 'text': fs.delete_path })
				).inject(fs_section);
			}
			if(fs.old_folder_cleanup){
				new Element('div.audit_preview_note', {
					'text': 'Old folder will be removed if empty.'
				}).inject(fs_section);
			}
		}

		// Database changes
		if(preview && preview.changes && preview.changes.database){
			var db = preview.changes.database;
			var db_section = new Element('div.audit_preview_section').inject(body);
			new Element('div.audit_preview_title', { 'text': 'Database Changes' }).inject(db_section);

			// Render all db fields except reset_status (handled by dropdown)
			Object.each(db, function(val, key){
				if(key === 'reset_status') return;
				if(val === null || val === undefined) return;
				if(typeof val === 'object'){
					val = JSON.stringify(val);
				}
				new Element('div.audit_kv_row').adopt(
					new Element('span.audit_kv_key', { 'text': key + ':' }),
					new Element('span.audit_kv_val', { 'text': String(val) })
				).inject(db_section);
			});

			// Status dropdown for reset_status
			if(db.reset_status){
				var status_row = new Element('div.audit_status_row').inject(db_section);
				new Element('span.audit_status_label', { 'text': 'Original movie status:' }).inject(status_row);
				var status_select = new Element('select.audit_status_select', {
					'events': { 'click': function(e){ e.stop(); } }
				}).inject(status_row);
				var default_status = db.reset_status['default'] || 'wanted';
				var status_labels = {
					'wanted': 'Set to Wanted',
					'done': 'Set to Done',
					'nochange': 'No change',
					'remove': 'Remove from database'
				};
				var options = db.reset_status['options'] || ['wanted', 'done', 'nochange'];
				options.each(function(val){
					var attrs = { 'value': val, 'text': status_labels[val] || val };
					if(val === default_status) attrs['selected'] = 'selected';
					new Element('option', attrs).inject(status_select);
				});
			}
		}

		// Warnings
		if(preview && preview.warnings && preview.warnings.length > 0){
			var warn_section = new Element('div.audit_preview_section.audit_preview_warnings').inject(body);
			new Element('div.audit_preview_title', { 'text': 'Warnings' }).inject(warn_section);
			preview.warnings.each(function(w){
				new Element('div.audit_preview_warning', { 'text': w }).inject(warn_section);
			});
		}

		// Footer buttons
		var footer = new Element('div.audit_modal_footer').inject(modal);

		new Element('a.audit_action_btn.secondary', {
			'text': 'Cancel',
			'events': { 'click': self.closePreviewModal.bind(self) }
		}).inject(footer);

		new Element('a.audit_action_btn.primary', {
			'text': 'Confirm & Execute',
			'events': { 'click': function(e){
				e.stop();
				var reset_status = status_select ? status_select.get('value') : '';
				self.executeFix(item_id, action, reset_status);
			}}
		}).inject(footer);

		// Allow Enter key to trigger Confirm & Execute
		self._modal_keydown = function(e){
			if(e.key === 'enter'){
				e.stop();
				var reset_status = status_select ? status_select.get('value') : '';
				self.executeFix(item_id, action, reset_status);
			}
		};
		document.addEvent('keydown', self._modal_keydown);
	},

	executeFix: function(item_id, action, reset_status){
		var self = this;

		var data = { 'item_id': item_id, 'action': action, 'confirm': 1 };
		if(reset_status){
			data['reset_status'] = reset_status;
		}

		// Check if we're acting on the top card before the list reloads
		var first = self.items_container.getFirst('.audit_item');
		if(first && first.get('data-item-id') === item_id){
			self._expand_next = true;
		}

		Api.request('audit.fix', {
			'data': data,
			'onComplete': function(json){
				self.closePreviewModal();

				if(json && json.success !== false){
					// Refresh results
					self.loadStats();
				} else {
					alert('Fix failed: ' + (json ? json.error || 'Unknown error' : 'No response'));
				}
			}
		});
	},

	ignoreItem: function(item_id, card){
		var self = this;

		// Check if we're acting on the top card before it gets removed
		var first = self.items_container.getFirst('.audit_item');
		if(first && first === card){
			self._expand_next = true;
		}

		Api.request('audit.ignore', {
			'data': { 'item_id': item_id },
			'onComplete': function(json){
				if(json && json.success){
					// Remove card with fade animation
					card.setStyle('opacity', 0);
					(function(){
						card.destroy();
						self.loadStats();
					}).delay(300);
				} else {
					alert('Ignore failed: ' + (json ? json.error || 'Unknown error' : 'No response'));
				}
			}
		});
	},

	closePreviewModal: function(){
		var self = this;
		if(self._modal_keydown){
			document.removeEvent('keydown', self._modal_keydown);
			self._modal_keydown = null;
		}
		self.preview_modal.setStyle('display', 'none');
		self.preview_modal.empty();
	},

	runTier2: function(item_id, btn, card){
		var self = this;

		// Show loading state
		var orig_text = btn.get('text');
		btn.set('text', 'Running...');
		btn.addClass('audit_tier2_running');
		btn.removeEvents('click');

		Api.request('audit.identify', {
			'data': { 'item_id': item_id },
			'onComplete': function(json){
				btn.removeClass('audit_tier2_running');

				if(json && json.success){
					self.loadStats();
				} else {
					btn.set('text', orig_text);
					btn.addEvent('click', function(e){
						e.stop();
						self.runTier2(item_id, btn, card);
					});
					alert('Tier 2 failed: ' + (json ? json.error || 'Unknown error' : 'No response'));
				}
			}
		});
	},

	manualReassign: function(item_id, input_el, btn){
		var self = this;
		var imdb_id = input_el.get('value').trim();

		if(!imdb_id){
			alert('Enter an IMDB ID (e.g. tt1234567)');
			return;
		}

		// Auto-prefix tt if user just typed numbers
		if(/^\d+$/.test(imdb_id)){
			imdb_id = 'tt' + imdb_id;
		}

		if(!/^tt\d{5,}$/.test(imdb_id)){
			alert('Invalid IMDB ID format. Expected tt1234567.');
			return;
		}

		var orig_text = btn.get('text');
		btn.set('text', 'Looking up...');
		btn.addClass('audit_tier2_running');

		Api.request('audit.reassign', {
			'data': { 'item_id': item_id, 'imdb_id': imdb_id },
			'onComplete': function(json){
				btn.removeClass('audit_tier2_running');
				btn.set('text', orig_text);

				if(json && json.success && json.preview){
					self.renderPreviewModal(json.preview, item_id, 'reassign_movie');
				} else {
					alert('Reassign failed: ' + (json ? json.error || 'Unknown error' : 'No response'));
				}
			}
		});
	},

	// -----------------------------------------------------------------------
	// Helper Functions
	// -----------------------------------------------------------------------

	formatNumber: function(n){
		if(n === null || n === undefined) return '0';
		return String(n).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
	},

	formatBytes: function(bytes){
		if(!bytes) return '0 B';
		var units = ['B', 'KB', 'MB', 'GB', 'TB'];
		var i = 0;
		while(bytes >= 1024 && i < units.length - 1){
			bytes /= 1024;
			i++;
		}
		return (i === 0 ? bytes : bytes.toFixed(1)) + ' ' + units[i];
	},

	flagIcon: function(check){
		var icons = {
			'resolution': 'RES',
			'runtime': 'RT',
			'title': 'TIT',
			'tv_episode': 'TV',
			'edition': 'ED',
			'template': 'TPL'
		};
		return icons[check] || check;
	},

	actionLabel: function(action){
		var labels = {
			'rename_template': 'Rename to Template',
			'rename_resolution': 'Rename Resolution',
			'rename_edition': 'Rename Edition',
			'delete_wrong': 'Delete',
			'reassign_movie': 'Reassign Movie',
			'needs_tier2': 'Needs Tier 2',
			'manual_review': 'Manual Review',
			'none': 'None'
		};
		return labels[action] || action;
	},

	getAlternativeActions: function(item){
		var actions = [];
		if(!item.flags) return actions;

		var has = {};
		item.flags.each(function(f){ has[f.check] = true; });

		if(has['template']) actions.push('rename_template');
		if(has['resolution']) actions.push('rename_resolution');
		if(has['edition']) actions.push('rename_edition');
		if(has['tv_episode']) actions.push('delete_wrong');
		if(has['title'] && item.identification && item.identification.method !== 'skipped')
			actions.push('reassign_movie');

		return actions;
	}

});

window.addEvent('domready', function(){
	new AuditSettingTab();
});
