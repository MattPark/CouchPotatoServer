config = [{
    'name': 'automation_providers',
    'groups': [
        {
            'label': 'Watchlists',
            'description': 'Check watchlists for new movies',
            'type': 'list',
            'name': 'watchlist_providers',
            'tab': 'automation',
            'order': 10,
            'options': [],
        },
        {
            'label': 'Popular Charts',
            'description': 'Automatically add popular and trending movies (configure minimal requirements below)',
            'type': 'list',
            'name': 'automation_providers',
            'tab': 'automation',
            'order': 20,
            'options': [],
        },
    ],
}]
