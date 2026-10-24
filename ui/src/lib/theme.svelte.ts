// The reader's appearance choice as shared state: the top bar's switch, the
// menu drawer's copy of it and the command palette all read and change this
// one value. The functions underneath are $lib/theme.ts's; this only holds
// the choice so three controls cannot disagree about which is pressed.
import { applyTheme, rememberTheme, rememberedTheme, type Theme } from './theme';

class Appearance {
	choice = $state<Theme>('system');

	/** Read the stored choice; called once in the browser, after hydration. */
	load(): void {
		this.choice = rememberedTheme();
	}

	set(next: Theme): void {
		this.choice = next;
		rememberTheme(next);
		applyTheme(next);
	}
}

export const appearance = new Appearance();
