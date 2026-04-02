var MoviesManage = new Class({

	Extends: PageBase,

	order: 20,
	name: 'manage',
	title: 'Do stuff to your existing movies!',

	indexAction: function(){
		var self = this;

		if(!self.list){

			// Menu items (kept in dots menu for backward compat)
			self.refresh_button = new Element('a', {
				'title': 'Rescan your library for new movies',
				'text': 'Full library refresh',
				'events':{
					'click': self.refresh.bind(self, true)
				}
			});

			self.refresh_quick = new Element('a', {
				'title': 'Just scan for recently changed',
				'text': 'Quick library scan',
				'events':{
					'click': self.refresh.bind(self, false)
				}
			});

			self.list = new MovieList({
				'identifier': 'manage',
				'filter': {
					'status': 'done',
					'release_status': 'done',
					'status_or': 1
				},
				'actions': [MA.IMDB, MA.Files, MA.Trailer, MA.Readd, MA.Delete],
				'menu': [self.refresh_button, self.refresh_quick],
				'on_empty_element': new Element('div.empty_manage').adopt(
					new Element('div', {
						'text': 'Seems like you don\'t have anything in your library yet. Add your existing movie folders in '
					}).grab(
						new Element('a', {
							'text': 'Settings > Manage',
							'href': App.createUrl('settings/manage')
						})
					),
					new Element('div.after_manage', {
						'text': 'When you\'ve done that, hit this button → '
					}).grab(
						new Element('a.button.green', {
							'text': 'Hit me, but not too hard',
							'events':{
								'click': self.refresh.bind(self, true)
							}
						})
					)
				)
			});
			$(self.list).inject(self.content);

			// Create scan toolbar above the movie list
			self.createScanToolbar();

			// Check if scan is in progress
			self.startProgressInterval();
		}

	},

	createScanToolbar: function(){
		var self = this;

		self.scan_toolbar = new Element('div.scan_toolbar').inject(self.list, 'top');

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

		self.progress_interval = requestInterval(function(){

			if(self.progress_request && self.progress_request.running)
				return;

			self.update_in_progress = true;
			self.setButtonsDisabled(true);

			self.progress_request = Api.request('manage.progress', {
				'onComplete': function(json){

					if(!json || !json.progress){
						clearRequestInterval(self.progress_interval);
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

						self.list.update();
					}
					else {
						// Capture progress so we can use it in our *each* closure
						var progress = json.progress;

						// Don't add loader when page is loading still
						if(!self.list.navigation)
							return;

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
									(folder_progress.eta > 0 ? ', ' + new Date ().increment('second', folder_progress.eta).timeDiffInWords().replace('from now', 'to go') : '')
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

	parseProgress: function (progress_object) {
		var folder, temp_array = [];

		for (folder in progress_object) {
			if (progress_object.hasOwnProperty(folder)) {
				temp_array.push(folder);
			}
		}
		return temp_array.stableSort();
	}

});
