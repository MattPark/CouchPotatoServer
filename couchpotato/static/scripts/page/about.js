var AboutSettingTab = new Class({

	tab: '',
	content: '',

	initialize: function(){
		var self = this;

		App.addEvent('loadSettings', self.addSettings.bind(self));

	},

	addSettings: function(){
		var self = this;

		self.settings = App.getPage('Settings');
		self.settings.addEvent('create', function(){
			var tab = self.settings.createTab('about', {
				'label': 'About',
				'name': 'about'
			});

			self.tab = tab.tab;
			self.content = tab.content;

			self.createAbout();

		});

		self.settings.default_action = 'about';
		// WebUI Feature:
		self.hide_about_dirs = !! App.options && App.options.webui_feature && App.options.webui_feature.hide_about_dirs;
		self.hide_about_update = !! App.options && App.options.webui_feature && App.options.webui_feature.hide_about_update;
	},

	createAbout: function(){
		var self = this;

		var about_block;
		self.settings.createGroup({
			'label': 'About This CouchPotato',
			'name': 'variables'
		}).inject(self.content).adopt(
			(about_block = new Element('dl.info')).adopt(
				new Element('dt[text=Commit]'),
				self.version_text = new Element('dd.version', {
					'text': 'Loading...'
				}),

				new Element('dt[text=Branch]'),
				self.branch_text = new Element('dd.branch'),

				new Element('dt[text=Updater]'),
				self.updater_type = new Element('dd.updater'),

				new Element('dt[text=PID]'),
				new Element('dd', {'text': App.getOption('pid')})
			)
		);

		if (!self.hide_about_update){
			self.version_text.addEvents({
				'click': App.checkForUpdate.bind(App, function(json){
					self.fillVersion(json.info);
				}),
				'mouseenter': function(){
					this.set('text', 'Check for updates');
				},
				'mouseleave': function(){
					self.fillVersion(Updater.getInfo());
				}
			});
		} else {
			// override cursor style from CSS
			self.version_text.setProperty('style', 'cursor: auto');
		}

		if (!self.hide_about_dirs){
			about_block.adopt(
				new Element('dt[text=Directories]'),
				new Element('dd', {'text': App.getOption('app_dir')}),
				new Element('dd', {'text': App.getOption('data_dir')}),
				new Element('dt[text=Startup Args]'),
				new Element('dd', {'html': App.getOption('args')}),
				new Element('dd', {'html': App.getOption('options')})
			);
		}

		if(!self.fillVersion(Updater.getInfo()))
			Updater.addEvent('loaded', self.fillVersion.bind(self));

	},

	fillVersion: function(json){
		if(!json || !json.version) return;
		var self = this;
		var v = json.version;
		var hash = v.hash || 'unknown';
		var dateStr = '';
		if (v.date) {
			var date = new Date(v.date * 1000);
			dateStr = ' (' + date.toLocaleString() + ')';
		}
		var repo = json.repo_name || 'MattPark/CouchPotatoServer';
		if (hash && hash !== 'unknown') {
			self.version_text.set('html', '<a href="https://github.com/' + repo + '/commit/' + hash + '" target="_blank">' + hash + '</a>' + dateStr);
		} else {
			self.version_text.set('text', hash + dateStr);
		}
		self.branch_text.set('text', json.branch || v.branch || 'unknown');
		self.updater_type.set('text', v.type || 'unknown');
	}

});

window.addEvent('domready', function(){
	new AboutSettingTab();
});
