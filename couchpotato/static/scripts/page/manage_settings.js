var ManageSettingTab = new Class({

	scan_toolbar: null,
	progress_container: null,
	results_container: null,
	quick_btn: null,
	full_btn: null,
	update_in_progress: false,
	progress_interval: null,
	progress_request: null,

	initialize: function(){
		var self = this;
		App.addEvent('loadSettings', self.addSettings.bind(self));
	},

	addSettings: function(){
		var self = this;

		self.settings = App.getPage('Settings');
		self.settings.addEvent('create', function(){
			// The manage tab is auto-created by the config framework.
			// Wait a tick for config groups to render, then inject the scan toolbar.
			requestTimeout(function(){
				var tab = self.settings.tabs['manage'];
				if (!tab) return;

				self.createScanToolbar(tab.content);

				// Check if a scan is already in progress
				self.startProgressInterval();
			}, 100);
		});
	},

	createScanToolbar: function(content){
		var self = this;

		// Create a settings-style group wrapper for the toolbar
		var group = self.settings.createGroup({
			'label': 'Library Scan',
			'name': 'library_scan'
		}).inject(content, 'top');

		self.scan_toolbar = new Element('div.scan_toolbar').inject(group);

		var buttons_row = new Element('div.scan_buttons').inject(self.scan_toolbar);

		// Quick scan button
		self.quick_btn = new Element('a.scan_btn.quick_scan', {
			'events': { 'click': self.refresh.bind(self, false) }
		}).adopt(
			new Element('span.scan_btn_icon.icon-search'),
			new Element('span.scan_btn_label', { 'text': 'Quick Scan' }),
			new Element('span.scan_btn_desc', { 'text': 'Check for recently added or changed movies' })
		).inject(buttons_row);

		// Full refresh button
		self.full_btn = new Element('a.scan_btn.full_refresh', {
			'events': { 'click': self.refresh.bind(self, true) }
		}).adopt(
			new Element('span.scan_btn_icon.icon-refresh'),
			new Element('span.scan_btn_label', { 'text': 'Full Library Refresh' }),
			new Element('span.scan_btn_desc', { 'text': 'Rescan all library folders from scratch' })
		).inject(buttons_row);

		// Progress area (hidden initially)
		self.progress_container = new Element('div.scan_progress').inject(self.scan_toolbar);
		self.progress_container.setStyle('display', 'none');

		// Results area (hidden initially)
		self.results_container = new Element('div.scan_results').inject(self.scan_toolbar);
		self.results_container.setStyle('display', 'none');
	},

	refresh: function(full){
		var self = this;

		if(!self.update_in_progress){

			Api.request('manage.update', {
				'data': {
					'full': +full
				}
			});

			// Clear previous results
			if(self.results_container){
				self.results_container.setStyle('display', 'none');
				self.results_container.empty();
			}

			self.startProgressInterval();

		}
	},

	setButtonsDisabled: function(disabled){
		var self = this;
		if(self.quick_btn){
			if(disabled){
				self.quick_btn.addClass('disabled');
				self.full_btn.addClass('disabled');
			} else {
				self.quick_btn.removeClass('disabled');
				self.full_btn.removeClass('disabled');
			}
		}
	},

	startProgressInterval: function(){
		var self = this;

		if(self.progress_interval)
			clearRequestInterval(self.progress_interval);

		self.progress_interval = requestInterval(function(){

			if(self.progress_request && self.progress_request.running)
				return;

			self.progress_request = Api.request('manage.progress', {
				'onComplete': function(json){

					if(!json || !json.progress){
						clearRequestInterval(self.progress_interval);
						self.progress_interval = null;
						self.update_in_progress = false;
						self.setButtonsDisabled(false);

						if(self.progress_container){
							self.progress_container.setStyle('display', 'none');
							self.progress_container.empty();
						}

						// Show results if available
						if(json && json.results && !json.results.scanning){
							self.showResults(json.results);
						}
					}
					else {
						self.update_in_progress = true;
						self.setButtonsDisabled(true);

						var progress = json.progress;

						self.progress_container.setStyle('display', '');
						self.progress_container.empty();

						var status_line = new Element('div.scan_status').adopt(
							new Element('span.scan_status_icon.icon-refresh.spinning'),
							new Element('span', { 'text': 'Scanning library...' })
						).inject(self.progress_container);

						// Show results-so-far if available
						if(json.results && json.results.scanning){
							new Element('span.scan_live_count', {
								'text': ' (' + json.results.movies_found + ' found, ' + json.results.movies_added + ' added)'
							}).inject(status_line);
						}

						var sorted_table = self.parseProgress(json.progress);

						sorted_table.each(function(folder){
							var folder_progress = progress[folder];
							new Element('div.scan_folder_row').adopt(
								new Element('span.folder', {'text': folder +
									(folder_progress.eta > 0 ? ', ' + new Date().increment('second', folder_progress.eta).timeDiffInWords().replace('from now', 'to go') : '')
								}),
								new Element('span.percentage', {'text': folder_progress.total ? Math.round(((folder_progress.total-folder_progress.to_go)/folder_progress.total)*100) + '%' : '0%'})
							).inject(self.progress_container);
						});

					}
				}
			});

		}, 1000);
	},

	showResults: function(results){
		var self = this;
		if(!self.results_container) return;

		self.results_container.empty();
		self.results_container.setStyle('display', '');

		var elapsed = results.elapsed_seconds || 0;
		var time_str;
		if(elapsed < 60){
			time_str = Math.round(elapsed) + 's';
		} else {
			time_str = Math.round(elapsed / 60) + 'm ' + Math.round(elapsed % 60) + 's';
		}

		var type_label = results.scan_type === 'full' ? 'Full refresh' : 'Quick scan';

		new Element('div.scan_results_inner').adopt(
			new Element('span.scan_results_icon.icon-ok'),
			new Element('span.scan_results_text', {
				'text': type_label + ' complete: ' +
					results.movies_found + ' movies found, ' +
					results.movies_added + ' added to library. ' +
					results.folders_scanned + ' folder' + (results.folders_scanned !== 1 ? 's' : '') + ' scanned in ' + time_str + '.'
			}),
			new Element('a.scan_results_dismiss', {
				'text': 'dismiss',
				'events': {
					'click': function(){
						self.results_container.setStyle('display', 'none');
					}
				}
			})
		).inject(self.results_container);
	},

	parseProgress: function(progress_object){
		var folder, temp_array = [];

		for (folder in progress_object) {
			if (progress_object.hasOwnProperty(folder)) {
				temp_array.push(folder);
			}
		}
		return temp_array.stableSort();
	}

});

window.addEvent('domready', function(){
	new ManageSettingTab();
});
