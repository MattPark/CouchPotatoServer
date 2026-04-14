var MetadataSettingTab = new Class({

	stats_panels: {},
	refresh_timer: null,

	initialize: function(){
		var self = this;
		App.addEvent('loadSettings', self.addSettings.bind(self));
	},

	addSettings: function(){
		var self = this;

		self.settings = App.getPage('Settings');
		self.settings.addEvent('create', function(){
			// The metadata tab is auto-created by the config framework.
			// Wait a tick for config groups to render, then inject stats panels.
			requestTimeout(function(){
				var tab = self.settings.tabs['metadata'];
				if (!tab) return;

				// Listen for tab activation to fetch stats
				tab.content.addEvent('activate', function(){
					self.refresh();
					self.startAutoRefresh();
				});

				self.injectStats(tab.content);
			}, 100);
		});
	},

	injectStats: function(content){
		var self = this;

		var services = ['omdbapi', 'themoviedb', 'fanarttv', 'opensubtitles'];
		services.each(function(name){
			var group = content.getElement('.group_' + name);
			if (group) {
				var panel = new Element('div.metadata_stats.stats_' + name, {
					'html': '<div class="stats_loading">Loading statistics...</div>'
				});
				panel.inject(group.getElement('h2'), 'after');
				self.stats_panels[name] = panel;
			}
		});
	},

	refresh: function(){
		var self = this;
		Api.request('metadata.stats', {
			'onComplete': function(json){
				if (json) self.renderStats(json);
			}
		});
	},

	startAutoRefresh: function(){
		var self = this;
		if (self.refresh_timer) clearInterval(self.refresh_timer);
		self.refresh_timer = setInterval(self.refresh.bind(self), 30000);
	},

	renderStats: function(json){
		var self = this;

		// OMDB
		if (json.omdb && self.stats_panels['omdbapi']) {
			var o = json.omdb;
			var hard_cap = o.hard_cap || o.budget;
			var soft_pct = hard_cap > 0 ? (o.budget / hard_cap * 100) : 100;
			var fill_pct = hard_cap > 0 ? Math.min(Math.round(o.calls_today / hard_cap * 100), 100) : 0;
			var tier_label = o.key_tier === 'patron' ? 'Patron' : 'Free';
			var bar_class, bar_text;

			if (o.rate_limited) {
				bar_class = 'bar_depleted';
				bar_text = 'Rate limited by OMDB (' + o.calls_today + ' / ' + hard_cap + ')';
			} else if (o.calls_today >= o.budget) {
				bar_class = 'bar_warning';
				bar_text = 'Soft budget reached (' + o.calls_today + ' / ' + o.budget + '), hard cap ' + hard_cap;
			} else if (o.calls_today > o.budget * 0.8) {
				bar_class = 'bar_caution';
				bar_text = o.calls_today + ' / ' + o.budget + ' budget (' + hard_cap + ' hard cap)';
			} else {
				bar_class = 'bar_ok';
				bar_text = o.calls_today + ' / ' + o.budget + ' budget (' + hard_cap + ' hard cap)';
			}

			self.stats_panels['omdbapi'].set('html',
				'<div class="stats_grid">' +
					'<div class="stat_item">' +
						'<span class="stat_value">' + o.calls_today + '</span>' +
						'<span class="stat_label">Calls Today</span>' +
					'</div>' +
					'<div class="stat_item">' +
						'<span class="stat_value">' + o.budget_remaining + '</span>' +
						'<span class="stat_label">Remaining</span>' +
					'</div>' +
					'<div class="stat_item">' +
						'<span class="stat_value">' + o.cache_hits_today + '</span>' +
						'<span class="stat_label">Cache Hits</span>' +
					'</div>' +
					'<div class="stat_item">' +
						'<span class="stat_value">' + tier_label + '</span>' +
						'<span class="stat_label">Tier</span>' +
					'</div>' +
				'</div>' +
				'<div class="stats_bar_wrap">' +
					'<div class="stats_bar_soft_mark" style="left:' + soft_pct + '%"></div>' +
					'<div class="stats_bar ' + bar_class + '" style="width:' + fill_pct + '%"></div>' +
					'<span class="stats_bar_text">' + bar_text + '</span>' +
				'</div>'
			);
		}

		// TMDB
		if (json.tmdb && self.stats_panels['themoviedb']) {
			var t = json.tmdb;
			self.stats_panels['themoviedb'].set('html',
				'<div class="stats_grid">' +
					'<div class="stat_item">' +
						'<span class="stat_value">' + t.calls_today + '</span>' +
						'<span class="stat_label">Calls Today</span>' +
					'</div>' +
					'<div class="stat_item">' +
						'<span class="stat_value">' + (t.has_custom_key ? 'Custom' : 'Built-in') + '</span>' +
						'<span class="stat_label">Key Type</span>' +
					'</div>' +
				'</div>'
			);
		}

		// FanartTV
		if (json.fanarttv && self.stats_panels['fanarttv']) {
			var f = json.fanarttv;
			self.stats_panels['fanarttv'].set('html',
				'<div class="stats_grid">' +
					'<div class="stat_item">' +
						'<span class="stat_value">' + f.calls_today + '</span>' +
						'<span class="stat_label">Calls Today</span>' +
					'</div>' +
					'<div class="stat_item">' +
						'<span class="stat_value">' + (f.has_custom_key ? 'Custom' : 'Built-in') + '</span>' +
						'<span class="stat_label">Key Type</span>' +
					'</div>' +
				'</div>'
			);
		}

		// OpenSubtitles
		if (json.opensubtitles && self.stats_panels['opensubtitles']) {
			var os = json.opensubtitles;
			self.stats_panels['opensubtitles'].set('html',
				'<div class="stats_grid">' +
					'<div class="stat_item">' +
						'<span class="stat_value">' + os.searches_today + '</span>' +
						'<span class="stat_label">Searches Today</span>' +
					'</div>' +
					'<div class="stat_item">' +
						'<span class="stat_value">' + os.hash_hits_today + '</span>' +
						'<span class="stat_label">Hash Hits</span>' +
					'</div>' +
					'<div class="stat_item">' +
						'<span class="stat_value">' + (os.has_api_key ? 'Configured' : 'Not Set') + '</span>' +
						'<span class="stat_label">API Key</span>' +
					'</div>' +
				'</div>'
			);
		}
	}

});

window.addEvent('domready', function(){
	new MetadataSettingTab();
});
